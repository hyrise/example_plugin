#include "index_tuner.hpp"

#include <numeric>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

void IndexTuner::create_indexes_for_workload(const Workload& workload) const {
  auto index_candidates = _enumerate_index_candidates();
  auto index_assessments = _assess_index_candidates(workload, index_candidates);
  // auto index_choices = select_assessments_greedy(index_assessments, static_cast<size_t>(SCALE_FACTOR * 30'000'000));
}

size_t sum_input_rows(size_t lhs, const ScanAccess& rhs) {
  return lhs + rhs.input_rows;
}

// This evaluator assigns a desirability according to the number of processed rows of this column
std::vector<AbstractCandidateAssessment> IndexTuner::_assess_index_candidates(const Workload& workload, std::vector<IndexCandidate>& index_candidates) const {
  std::vector<AbstractCandidateAssessment> index_candidate_assessments;

  for (auto& index_candidate : index_candidates) {
    const auto& identifier = index_candidate.identifier;

    std::cout << identifier << std::endl;

    const auto scan_accesses = workload.get_column_accesses(index_candidate.identifier).scan_accesses;
    const auto total_processed_rows = std::accumulate(scan_accesses.rbegin(), scan_accesses.rend(), 0, sum_input_rows);

    const auto& table_name = identifier.first;
    const auto& column_id = identifier.second;
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
        const TableColumnIdentifier identifier = std::make_pair(table_name, next_attribute_id);
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
