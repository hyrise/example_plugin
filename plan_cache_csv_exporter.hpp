#pragma once

#include <boost/bimap.hpp>

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

// typedef boost::bimap<std::string, uint16_t> table_name_id_bimap;
// typedef table_name_id_bimap::value_type table_name_id;

class PlanCacheCsvExporter {
 public:
  PlanCacheCsvExporter();
 private:
  StorageManager& _sm;

  std::string _process_table_scan(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  std::string _process_validate(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  std::string _process_aggregate(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  std::string _process_projection(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  void _process_index_scan(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  std::string _process_join(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
  void _process_pqp(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash);
};

}  // namespace opossum
