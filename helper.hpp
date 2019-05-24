#pragma once

#include <chrono>
#include <unordered_map>
#include <vector>

#include "types.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/index/base_index.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

class OperatorAccess {
 public:
  std::chrono::nanoseconds execution_time;
  std::time_t execution_timestamp;

  OperatorAccess(std::chrono::nanoseconds execution_time, std::time_t execution_timestamp) : execution_time(execution_time), execution_timestamp(execution_timestamp) {}
};

class JoinAccess : public OperatorAccess {
 public:
  size_t input_rows_left;
  size_t input_rows_right;
  size_t output_rows;
};

class ScanAccess : public OperatorAccess {
 public:
  size_t input_rows;
  size_t output_rows;

  ScanAccess(std::chrono::nanoseconds execution_time, std::time_t execution_timestamp, size_t input_rows, size_t output_rows) : OperatorAccess(execution_time, execution_timestamp), input_rows(input_rows), output_rows(output_rows) {}
};

struct ColumnAccesses {
  std::vector<JoinAccess> join_accesses;
  std::vector<ScanAccess> scan_accesses;
};

struct TableColumnIdentifier {
  std::string table_name;
  opossum::ColumnID column_id;

  TableColumnIdentifier(const std::string& table_name, const opossum::ColumnID column_id) : table_name(table_name), column_id(column_id) {}

  bool operator== (const TableColumnIdentifier& rhs) const {
    const std::string combined_lhs = this->table_name + "_" + std::to_string(this->column_id);
    const std::string combined_rhs = rhs.table_name + "_" + std::to_string(rhs.column_id);

    return combined_lhs == combined_rhs;
  }
};

std::ostream &operator<<(std::ostream& os, const TableColumnIdentifier& identifier);

namespace std {
template <>
struct hash<TableColumnIdentifier> {
  size_t operator()(TableColumnIdentifier const& id) const {
    const std::string combined = id.table_name + "_" + std::to_string(id.column_id);
    return std::hash<std::string>{}(combined);
  }
};
}  // namespace std

class Workload {
 public:
  void add_access(const TableColumnIdentifier& identifier, ScanAccess access) {
    _accesses[identifier].scan_accesses.push_back(access);
  }

  size_t count_accesses(const TableColumnIdentifier& identifier) const {
    size_t count = 0;
    
    count += _accesses.at(identifier).join_accesses.size();
    count += _accesses.at(identifier).scan_accesses.size();

    return count;
  }

  ColumnAccesses get_column_accesses(const TableColumnIdentifier& identifier) const {
    return _accesses.at(identifier);
  }

  // Only for debugging
  std::unordered_map<TableColumnIdentifier, ColumnAccesses> get_accesses() const {
    return _accesses;
  }

  bool contains_accesses(const TableColumnIdentifier& identifier) const {
    return _accesses.count(identifier) > 0;
  }

 private:
  std::unordered_map<TableColumnIdentifier, ColumnAccesses> _accesses;
};

class AbstractCandidate {
public:
  TableColumnIdentifier identifier;

  AbstractCandidate(TableColumnIdentifier identifier) : identifier(identifier) {}
};

class IndexCandidate : public AbstractCandidate {
public:
  IndexCandidate(TableColumnIdentifier identifier_sub) : AbstractCandidate(identifier_sub) {}
};

class AbstractCandidateAssessment {
public:
  std::shared_ptr<AbstractCandidate> candidate;
  float desirability = 0.0f;
  float cost = 0.0f;

  AbstractCandidateAssessment(std::shared_ptr<AbstractCandidate> candidate, float desirability, float cost = 0.0f) : candidate(candidate), desirability(desirability), cost(cost) {}
};

class IndexCandidateAssessment : public AbstractCandidateAssessment {
public:
  IndexCandidateAssessment(const std::shared_ptr<IndexCandidate> candidate_2, const float desirability_2, const float cost_2) : AbstractCandidateAssessment(candidate_2, desirability_2, cost_2) {}
};

size_t predict_index_size(const std::string& table_name, opossum::ColumnID column_id);
