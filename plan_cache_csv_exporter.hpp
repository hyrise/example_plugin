#pragma once

#include <boost/bimap.hpp>

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace {

std::string wrap_string(const std::string str) {
  return std::string{"\""} + str + "\"";
}

} // namespace

namespace opossum {

struct SingleTableScan {
  std::string query_hash{};
  // TODO: scan hash for column vs column 
  std::string scan_type{};
  std::string table_name{};
  std::string column_name{};
  size_t input_rows{};
  size_t output_rows{};
  size_t runtime_ns{};
  std::string description{};

  std::vector<std::string> string_vector() const {
    std::vector<std::string> result;

    result.emplace_back(wrap_string(query_hash));
    result.emplace_back(wrap_string(scan_type));
    result.emplace_back(wrap_string(table_name));
    result.emplace_back(wrap_string(column_name));
    result.emplace_back(std::to_string(input_rows));
    result.emplace_back(std::to_string(output_rows));
    result.emplace_back(std::to_string(runtime_ns));
    result.emplace_back(wrap_string(description));

    return result;
  }
};

struct WorkloadTableScans {
  std::string csv_header{"QUERY_HASH|COLUMN_TYPE|TABLE_NAME|COLUMN_NAME|INPUT_ROWS|OUTPUT_ROWS|RUNTIME_NS|DESCRIPTION"};
  std::vector<SingleTableScan> scans;
};

struct SingleProjection {
  std::string query_hash{};
  std::string projection_hash{};
  std::string column_type{};
  std::string table_name{};
  std::string column_name{};
  size_t input_rows{};
  size_t output_rows{};
  size_t runtime_ns{};
  std::string description{};

  std::vector<std::string> string_vector() const {
    std::vector<std::string> result;

    result.emplace_back(wrap_string(query_hash));
    result.emplace_back(wrap_string(scan_type));
    result.emplace_back(wrap_string(table_name));
    result.emplace_back(wrap_string(column_name));
    result.emplace_back(std::to_string(input_rows));
    result.emplace_back(std::to_string(output_rows));
    result.emplace_back(std::to_string(runtime_ns));
    result.emplace_back(wrap_string(description));

    return result;
  }
};

struct WorkloadProjections {
  std::string csv_header{"QUERY_HASH|PROJECTION_HASH|COLUMN_TYPE|TABLE_NAME|COLUMN_NAME|INPUT_ROWS|OUTPUT_ROWS|RUNTIME_NS|DESCRIPTION"};
  std::vector<SingleProjection> scans;
};

class PlanCacheCsvExporter {
 public:
  PlanCacheCsvExporter(const std::string export_folder_name);
  void run();
  void write_to_disk() const;
 private:
  StorageManager& _sm;

  void _process_table_scan(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  std::string _process_validate(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  std::string _process_aggregate(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  void _process_projection(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  void _process_index_scan(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  std::string _process_join(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);

  void _process_pqp(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash);
  void _extract_physical_query_plan_cache_data() const;

  std::string _export_folder_name;
  WorkloadTableScans _table_scans;
  WorkloadProjections _projections;
};

}  // namespace opossum
