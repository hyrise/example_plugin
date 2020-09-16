#pragma once

#include <boost/bimap.hpp>

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace {

std::string wrap_string_old(const std::string str) {
  return std::string{"\""} + str + "\"";
}

} // namespace

namespace opossum {

struct SingleGetTable {
  std::string query_hash{};
  std::string operator_hash{};
  std::string left_input_operator{};
  std::string right_input_operator{};
  std::string table_name{};
  size_t pruned_chunk_count{};
  size_t pruned_column_count{};
  size_t output_chunk_count{};
  size_t output_row_count{};
  size_t runtime_ns{};
  std::string description{};

  std::vector<std::string> string_vector() const {
    std::vector<std::string> result;

    result.emplace_back("GET_TABLE");
    result.emplace_back(wrap_string_old(query_hash));
    result.emplace_back(wrap_string_old(operator_hash));
    result.emplace_back(wrap_string_old(left_input_operator));
    result.emplace_back(wrap_string_old(right_input_operator));
    result.emplace_back(wrap_string_old(table_name));
    result.emplace_back(std::to_string(pruned_chunk_count));
    result.emplace_back(std::to_string(pruned_column_count));
    result.emplace_back(std::to_string(output_chunk_count));
    result.emplace_back(std::to_string(output_row_count));
    result.emplace_back(std::to_string(runtime_ns));
    result.emplace_back(wrap_string_old(description));

    return result;
  }
};

struct WorkloadGetTables {
  std::string csv_header() const {
    return "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|TABLE_NAME|PRUNED_CHUNK_COUNT|PRUNED_COLUMN_COUNT|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|RUNTIME_NS|DESCRIPTION";
  }

  std::vector<SingleGetTable> instances;
};

struct SingleTableScan {
  std::string query_hash{};
  std::string operator_hash{};
  std::string left_input_operator{};
  std::string right_input_operator{};
  std::string scan_type{};
  std::string table_name{};
  std::string column_name{};
  std::string predicate_condition{};
  size_t scans_skipped{};
  size_t scans_sorted{};
  size_t input_chunk_count{};
  size_t input_row_count{};
  size_t output_chunk_count{};
  size_t output_row_count{};
  size_t runtime_ns{};
  std::string description{};

  std::vector<std::string> string_vector() const {
    std::vector<std::string> result;

    result.emplace_back("TABLE_SCAN");
    result.emplace_back(wrap_string_old(query_hash));
    result.emplace_back(wrap_string_old(operator_hash));
    result.emplace_back(wrap_string_old(left_input_operator));
    result.emplace_back(wrap_string_old(right_input_operator));
    result.emplace_back(wrap_string_old(scan_type));
    result.emplace_back(wrap_string_old(table_name));
    result.emplace_back(wrap_string_old(column_name));
    result.emplace_back(wrap_string_old(predicate_condition));
    result.emplace_back(std::to_string(scans_skipped));
    result.emplace_back(std::to_string(scans_sorted));
    result.emplace_back(std::to_string(input_chunk_count));
    result.emplace_back(std::to_string(input_row_count));
    result.emplace_back(std::to_string(output_chunk_count));
    result.emplace_back(std::to_string(output_row_count));
    result.emplace_back(std::to_string(runtime_ns));
    result.emplace_back(wrap_string_old(description));

    return result;
  }
};

struct WorkloadTableScans {
  std::string csv_header() const {
    return "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|COLUMN_TYPE|TABLE_NAME|COLUMN_NAME|PREDICATE_CONDITION|SCANS_SKIPPED|SCANS_SORTED|INPUT_CHUNK_COUNT|INPUT_ROW_COUNT|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|RUNTIME_NS|DESCRIPTION";
  }

  std::vector<SingleTableScan> instances;
};

struct SingleProjection {
  std::string query_hash{};
  std::string operator_hash{};
  std::string left_input_operator{};
  std::string right_input_operator{};
  std::string column_type{};
  std::string table_name{};
  std::string column_name{};
  size_t input_chunk_count{};
  size_t input_row_count{};
  size_t output_chunk_count{};
  size_t output_row_count{};
  size_t runtime_ns{};
  std::string description{};

  std::vector<std::string> string_vector() const {
    std::vector<std::string> result;

    result.emplace_back("PROJECTION");
    result.emplace_back(wrap_string_old(query_hash));
    result.emplace_back(wrap_string_old(operator_hash));
    result.emplace_back(wrap_string_old(left_input_operator));
    result.emplace_back(wrap_string_old(right_input_operator));
    result.emplace_back(wrap_string_old(column_type));
    result.emplace_back(wrap_string_old(table_name));
    result.emplace_back(wrap_string_old(column_name));
    result.emplace_back(std::to_string(input_chunk_count));
    result.emplace_back(std::to_string(input_row_count));
    result.emplace_back(std::to_string(output_chunk_count));
    result.emplace_back(std::to_string(output_row_count));
    result.emplace_back(std::to_string(runtime_ns));
    result.emplace_back(wrap_string_old(description));

    return result;
  }
};

struct WorkloadProjections {
  std::string csv_header() const {
    return "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|COLUMN_TYPE|TABLE_NAME|COLUMN_NAME|INPUT_CHUNK_COUNT|INPUT_ROW_COUNT|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|RUNTIME_NS|DESCRIPTION";
  }

  std::vector<SingleProjection> instances;
};

// struct SingleAggregate {
//   std::string query_hash{};
//   std::string operator_hash{};
//   std::string left_input_operator{};
//   std::string right_input_operator{};
//   std::string scan_type{};
//   std::string table_name{};
//   std::string column_name{};
//   std::string predicate_condition{};
//   size_t scans_skipped{};
//   size_t scans_sorted{};
//   size_t input_chunk_count{};
//   size_t input_row_count{};
//   size_t output_chunk_count{};
//   size_t output_row_count{};
//   std::map<std::string, size_t> step_runtimes;
//   size_t runtime_ns{};
//   std::string description{};

//   std::vector<std::string> string_vector() const {
//     std::vector<std::string> result;

//     result.emplace_back("AGGREGATE");
//     result.emplace_back(wrap_string_old(query_hash));
//     result.emplace_back(wrap_string_old(operator_hash));
//     result.emplace_back(wrap_string_old(left_input_operator));
//     result.emplace_back(wrap_string_old(right_input_operator));
//     result.emplace_back(wrap_string_old(scan_type));
//     result.emplace_back(wrap_string_old(table_name));
//     result.emplace_back(wrap_string_old(column_name));
//     result.emplace_back(wrap_string_old(predicate_condition));
//     result.emplace_back(std::to_string(scans_skipped));
//     result.emplace_back(std::to_string(scans_sorted));
//     result.emplace_back(std::to_string(input_chunk_count));
//     result.emplace_back(std::to_string(input_row_count));
//     result.emplace_back(std::to_string(output_chunk_count));
//     result.emplace_back(std::to_string(output_row_count));
//     for (const auto& [_, step_runtimes] : step_runtimes) {
//       result.emplace_back(std::to_string(step_runtimes));
//     }
//     result.emplace_back(std::to_string(runtime_ns));
//     result.emplace_back(wrap_string_old(description));

//     return result;
//   }
// };

// struct WorkloadAggregations {
//   std::string csv_header() {
//     std::stringstream header;
//     header << std::string{"OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|COLUMN_TYPE|TABLE_NAME|COLUMN_NAME|GROUP_BY_COLUMN_COUNT|AGGREGATE_COLUMN_COUNT|INPUT_CHUNK_COUNT|INPUT_ROW_COUNT|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|"};

//     // First, check that all runtime step maps have equal keys
//     for (auto instance_id = size_t{1}; instance_id < instances.size(); ++instance_id) {
//       const auto first = instances[instance_id - 1];
//       const auto second = instances[instance_id];
//       Assert(first.step_runtimes.size() == second.step_runtimes.size(), "Size mismatch of runtime steps vector.");
//       for (const auto& [step_name, _] : first.step_runtimes) {
//         Assert(second.contains(step_name), "Step runtimes do not match between instances.");
//       }
//     }

//     for (const auto& [step_name, _] : step_runtimes) {
//       header << std::to_string(step_name) + "|";
//     }

//     header << "RUNTIME_NS|DESCRIPTION\n";
//     return header.str();
//   }

//   std::vector<SingleAggregate> instances;
// };

class PlanCacheCsvExporter {
 public:
  PlanCacheCsvExporter(const std::string export_folder_name);
  void run();
  void write_to_disk() const;
  void write_map_to_disk(const std::string file_name, const std::map<std::string, std::vector<std::string>>& operator_instances) const;
 private:
  StorageManager& _sm;

  void _process_table_scan(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  void _process_get_table(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  std::string _process_general_operator(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  void _process_aggregate(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  void _process_projection(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  std::string _process_join(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);

  void _process_pqp(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash,
                    std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes);
  void _extract_physical_query_plan_cache_data() const;

  bool _operator_result_is_probably_materialized(const std::shared_ptr<const AbstractOperator>& op);

  std::string _export_folder_name;
  WorkloadTableScans _table_scans;
  WorkloadProjections _projections;
  WorkloadGetTables _get_tables;

  std::map<std::string, std::vector<std::string>> _aggregates{};
};

}  // namespace opossum
