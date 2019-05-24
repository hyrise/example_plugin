#include "index_tuner.hpp"

#include <numeric>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

void IndexTuner::create_indexes_for_workload(const Workload& workload, size_t budget) const {
  auto index_candidates = _enumerate_index_candidates();
  auto index_assessments = _assess_index_candidates(workload, index_candidates);
  auto index_choices = select_assessments_greedy(index_assessments, budget);
}

size_t sum_input_rows(size_t lhs, const ScanAccess& rhs) {
  return lhs + rhs.input_rows;
}

bool compareByDesirabilityPerCost(const AbstractCandidateAssessment& assessment_1, const AbstractCandidateAssessment& assessment_2) {
  const auto desirability_per_cost_1 = assessment_1.desirability / assessment_1.cost;
  const auto desirability_per_cost_2 = assessment_2.desirability / assessment_2.cost;

  return desirability_per_cost_1 > desirability_per_cost_2;
}

std::vector<AbstractCandidate> IndexTuner::select_assessments_greedy(std::vector<AbstractCandidateAssessment>& assessments, size_t budget) const {
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
std::vector<AbstractCandidateAssessment> IndexTuner::_assess_index_candidates(const Workload& workload, std::vector<IndexCandidate>& index_candidates) const {
  std::vector<AbstractCandidateAssessment> index_candidate_assessments;

  for (auto& index_candidate : index_candidates) {
    const auto& identifier = index_candidate.identifier;
    if (!workload.contains_accesses(identifier)) continue;

    const auto scan_accesses = workload.get_column_accesses(index_candidate.identifier).scan_accesses;
    const auto total_processed_rows = std::accumulate(scan_accesses.rbegin(), scan_accesses.rend(), 0, sum_input_rows);

    const auto& table_name = identifier.table_name;
    const auto& column_id = identifier.column_id;
    index_candidate_assessments.emplace_back(std::make_shared<IndexCandidate>(index_candidate), static_cast<float>(total_processed_rows), static_cast<float>(predict_index_size(table_name, column_id)));
  }

  return index_candidate_assessments;
}

std::vector<IndexCandidate> IndexTuner::_enumerate_index_candidates() const {
  std::vector<IndexCandidate> index_candidates;

  uint16_t next_table_id = 0;
  for (const auto& table_name : StorageManager::get().table_names()) {
    const auto table = StorageManager::get().get_table(table_name);
    if (table->row_count() >= (1'000)) {
      ColumnID next_attribute_id{0};
      for ([[maybe_unused]] const auto& column_def : table->column_definitions()) {
        const TableColumnIdentifier identifier = TableColumnIdentifier(table_name, next_attribute_id);
        next_attribute_id++;
        index_candidates.emplace_back(identifier);
      }
    } else {
      std::cout << "Not considering columns of: " << table_name << " as candidates." << std::endl;
    }

    ++next_table_id;
  }

  return index_candidates;
}

}  // namespace opossum
