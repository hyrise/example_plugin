#pragma once

#include <iostream>

#include "helper.hpp"

namespace opossum {

class IndexTuner {
 public:
  void create_indexes_for_workload(const Workload& workload) const;
 private:
  // This enumerator considers all columns except these of tables that have less than 10'000 * SCALE_FACTOR rows
  std::vector<IndexCandidate> _enumerate_index_candidates() const;

  // This evaluator assigns a desirability according to the number of processed rows of this column
  std::vector<AbstractCandidateAssessment> _assess_index_candidates(const Workload& workload, std::vector<IndexCandidate>& index_candidates) const;
};

}  // namespace opossum
