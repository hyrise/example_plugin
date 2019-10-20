#pragma once

#include <boost/bimap.hpp>

#include "operators/abstract_operator.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

#include "helper.hpp"

namespace opossum {

// typedef boost::bimap<std::string, uint16_t> table_name_id_bimap;
// typedef table_name_id_bimap::value_type table_name_id;

class WorkloadPredictor {
 public:
  WorkloadPredictor() : _sm(StorageManager::get()) {}
  const Workload get_forecasts();
 private:
  StorageManager& _sm;
  // table_name_id_bimap _table_name_id_map;
  // std::unordered_map<TableColumnIdentifier, std::string> _attribute_id_name_map;

  void _update_table_metadata();
  const Workload _calculate_forecast() const;
  std::string _process_table_scan(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash, const size_t frequency);
  std::string _process_validate(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash, const size_t frequency);
  std::string _process_aggregate(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash, const size_t frequency);
  std::string _process_projection(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash, const size_t frequency);
  void _process_index_scan(std::shared_ptr<const AbstractOperator> op, size_t frequency);
  std::string _process_join(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash, const size_t frequency);
  void _process_pqp(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash, const size_t frequency);

  Workload _last_workload;
};

}  // namespace opossum
