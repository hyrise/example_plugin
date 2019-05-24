#pragma once

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

using TableColumnIdentifier = std::pair<uint16_t, opossum::ColumnID>;

namespace std {
template <>
struct hash<TableColumnIdentifier> {
  size_t operator()(TableColumnIdentifier const& v) const {
    const std::string combined = std::to_string(v.first) + "_" + std::to_string(v.second);
    return std::hash<std::string>{}(combined);
  }
};
}  // namespace std

class Workload {
 public:
  void add_access(const TableColumnIdentifier& identifier, ScanAccess access) {
    _accesses[identifier].scan_accesses.push_back(access);
  }

  size_t count_accesses(const TableColumnIdentifier& identifier) {
    size_t count = 0;
    
    count += _accesses[identifier].join_accesses.size();
    count += _accesses[identifier].scan_accesses.size();

    return count;
  }

 private:
  std::unordered_map<TableColumnIdentifier, ColumnAccesses> _accesses;
};
