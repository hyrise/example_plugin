#pragma once

#include <iostream>
#include <fstream>

#include "helper.hpp"

namespace opossum {

class IndexTuner {
 public:
  void create_indexes_for_workload(const Workload& workload, size_t budget);

  IndexTuner();
 private:
  // This enumerator considers all columns except these of tables that have less than 10'000 * SCALE_FACTOR rows
  std::vector<IndexCandidate> _enumerate_index_candidates() const;

  // This evaluator assigns a desirability according to the number of processed rows of this column
  // TODO make this const again, it had to do with the _output
  std::vector<AbstractCandidateAssessment> _assess_index_candidates(const Workload& workload, std::vector<IndexCandidate>& index_candidates);

  // This selector greedily selects assessed items based on desirability per cost
  std::vector<AbstractCandidate> _select_assessments_greedily(std::vector<AbstractCandidateAssessment>& assessments, size_t budget) const;

  void _initialize_indexes(const std::vector<AbstractCandidate>& index_choices);

  std::vector<TableColumnIdentifier> _current_index_identifiers;

  std::ofstream _output;
};

}  // namespace opossum
