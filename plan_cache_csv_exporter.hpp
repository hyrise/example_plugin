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

struct TableScanInformation {
	std::string query_hash{};
	std::string scan_type{};
	std::string table_name{};
	std::string column_name{};
	size_t input_rows{};
	size_t output_rows{};
	size_t runtime_ns{};
	std::string description{};

	// TODO: use third party library?
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

class PlanCacheCsvExporter {
 public:
  PlanCacheCsvExporter();
  void write_to_disk();
 private:
  StorageManager& _sm;

  std::string _process_table_scan(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  void _process_table_scan2(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  std::string _process_validate(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  std::string _process_aggregate(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  std::string _process_projection(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  void _process_index_scan(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  std::string _process_join(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  void _process_pqp(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);

  std::vector<TableScanInformation> _table_scans;
};

}  // namespace opossum
