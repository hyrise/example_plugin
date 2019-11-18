#include "index_tuner_advanced.hpp"

#include <numeric>
#include <chrono>
#include <ctime>

#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

IndexTunerAdvanced::IndexTunerAdvanced() {
  _output.open("output_tuner_advanced");
}

void IndexTunerAdvanced::create_indexes_for_workload(const Workload& workload, size_t budget) {
  auto d = std::chrono::system_clock::now();
  std::time_t d_time = std::chrono::system_clock::to_time_t(d);
  _output << std::string(std::ctime(&d_time)).substr(0, 24) << ": Start Tuning" << std::endl;
  auto index_candidates = _enumerate_index_candidates();
  auto index_assessments = _assess_index_candidates(workload, index_candidates);
  auto index_choices = _select_assessments_greedily(index_assessments, budget);
  _initialize_indexes(index_choices);

  _output << std::endl << "####### Finish Tuning #######" << std::endl << std::endl;
}

void IndexTunerAdvanced::_initialize_indexes(const std::vector<AbstractCandidate>& index_choices) {
  std::vector<TableColumnIdentifier> new_index_identifiers;

  _output << std::endl << "Index Selection Phase:" << std::endl;
  for (const auto& index_choice : index_choices) {
    if (std::find(_current_index_identifiers.cbegin(), _current_index_identifiers.cend(), index_choice.identifier) != _current_index_identifiers.cend()) {
      _output << "Index already present: " << index_choice.identifier << std::endl;
      new_index_identifiers.push_back(index_choice.identifier);
      continue;
    }

    const auto& table_name = index_choice.identifier.table_name;
    const auto column_id = index_choice.identifier.column_id;
    const auto table = StorageManager::get().get_table(table_name);
    const std::string index_name = "idx_" + table_name + "_" + std::to_string(column_id);

    table->create_index<GroupKeyIndex>({column_id}, index_name);
    _output << "Created index on: " << index_choice.identifier << std::endl;
    new_index_identifiers.push_back(index_choice.identifier);
  }

  for (const auto& old_index_identifer : _current_index_identifiers) {
    if (std::find(new_index_identifiers.cbegin(), new_index_identifiers.cend(), old_index_identifer) == new_index_identifiers.cend()) {
      const auto& table_name = old_index_identifer.table_name;
      const auto column_id = old_index_identifer.column_id;
      const auto table = StorageManager::get().get_table(table_name);
      table->remove_index({column_id});

      _output << "Dropped index on: " << old_index_identifer << std::endl;
    }
  }
  _current_index_identifiers = new_index_identifiers;
}

size_t sum_frequency(size_t lhs, const ScanAccess& rhs) {
  return lhs + rhs.frequency;
}

bool compareByDesirabilityPerCost(const AbstractCandidateAssessment& assessment_1, const AbstractCandidateAssessment& assessment_2) {
  const auto desirability_per_cost_1 = assessment_1.desirability / assessment_1.cost;
  const auto desirability_per_cost_2 = assessment_2.desirability / assessment_2.cost;

  return desirability_per_cost_1 < desirability_per_cost_2;
}

std::vector<AbstractCandidate> IndexTunerAdvanced::_select_assessments_greedily(std::vector<AbstractCandidateAssessment>& assessments, size_t budget) const {
  std::vector<AbstractCandidate> selected_candidates;
  size_t used_budget = 0;

  auto assessments_copy = assessments;
  std::sort(assessments_copy.begin(), assessments_copy.end(), compareByDesirabilityPerCost);

  for (const auto& assessment : assessments_copy) {
    if (assessment.cost + used_budget < budget) {
      selected_candidates.emplace_back(*(assessment.candidate));
      used_budget += static_cast<size_t>(assessment.cost);
    }
  }

  return selected_candidates;
}

// This evaluator assigns a desirability according to the number of processed rows of this column
std::vector<AbstractCandidateAssessment> IndexTunerAdvanced::_assess_index_candidates(const Workload& workload, std::vector<IndexCandidate>& index_candidates) {
  std::vector<AbstractCandidateAssessment> index_candidate_assessments;

  _output << std::endl <<  "Index Assessment Phase:" << std::endl;
  for (auto& index_candidate : index_candidates) {
    const auto& identifier = index_candidate.identifier;
    if (!workload.contains_accesses(identifier)) continue;

    const auto scan_accesses = workload.get_column_accesses(index_candidate.identifier).scan_accesses;
    const auto total_frequency = std::accumulate(scan_accesses.rbegin(), scan_accesses.rend(), 0, sum_frequency);

    const auto& table_name = identifier.table_name;
    const auto& column_id = identifier.column_id;

    const float cost_reduction = _cost_table.at(identifier) * total_frequency;

    index_candidate_assessments.emplace_back(std::make_shared<IndexCandidate>(index_candidate), cost_reduction, static_cast<float>(predict_index_size(table_name, column_id)));

    _output << table_name << " " << std::to_string(column_id) << " Cost Reduction: " << std::to_string(cost_reduction) << " Predicted Index Size: " << std::to_string(predict_index_size(table_name, column_id)) << std::endl;
  }

  return index_candidate_assessments;
}

std::vector<IndexCandidate> IndexTunerAdvanced::_enumerate_index_candidates() const {
  std::vector<IndexCandidate> index_candidates;

  uint16_t next_table_id = 0;
  for (const auto& table_name : StorageManager::get().table_names()) {
    const auto table = StorageManager::get().get_table(table_name);
    if (table->row_count() >= (20'000)) {
      ColumnID next_attribute_id{0};
      for ([[maybe_unused]] const auto& column_def : table->column_definitions()) {
        const TableColumnIdentifier identifier = TableColumnIdentifier(table_name, next_attribute_id);
        next_attribute_id++;
        index_candidates.emplace_back(identifier);
      }
    } else {
      // std::cout << "Not considering columns of: " << table_name << " as candidates because of the table's size." << std::endl;
    }

    ++next_table_id;
  }

  return index_candidates;
}

}  // namespace opossum