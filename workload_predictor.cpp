#include "workload_predictor.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/table.hpp"

namespace opossum {

void WorkloadPredictor::_process_join(std::shared_ptr<const AbstractOperator> op, size_t frequency) {
    // const auto node = op->lqp_node();
    // const auto join_node = std::dynamic_pointer_cast<const JoinNode>(node);

    // if (join_node->join_mode == JoinMode::Inner) {
    //   const auto operator_predicate = OperatorJoinPredicate::from_expression(*(join_node->node_expressions[0]),
    //                                                                          *node->left_input(), *node->right_input());
    //   if (operator_predicate.has_value()) {
    //     if ((*operator_predicate).predicate_condition == PredicateCondition::Equals) {
    //       const auto column_expressions = join_node->column_expressions();
    //       const auto predicate_expression = std::dynamic_pointer_cast<const AbstractPredicateExpression>(join_node->node_expressions[0]);
    //       std::string table_name_0, table_name_1;
    //       ColumnID original_column_id_0, original_column_id_1;

    //       for (const auto& column_expression : column_expressions) {
    //         if (*column_expression == *(predicate_expression->arguments[0])) {
    //           if (column_expression->type == ExpressionType::LQPColumn) {
    //             const auto column_reference = std::dynamic_pointer_cast<LQPColumnExpression>(column_expression)->column_reference;
    //             original_column_id_0 = column_reference.original_column_id();

    //             const auto original_node_0 = column_reference.original_node();
    //             if (original_node_0->type == LQPNodeType::StoredTable) {
    //               const auto stored_table_node_0 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_0);
    //               table_name_0 = stored_table_node_0->table_name;
    //             }
    //           }
    //         }

    //         if (*column_expression == *(predicate_expression->arguments[1])) {
    //           if (column_expression->type == ExpressionType::LQPColumn) {
    //             const auto column_reference = std::dynamic_pointer_cast<LQPColumnExpression>(column_expression)->column_reference;
    //             original_column_id_1 = column_reference.original_column_id();
                
    //             const auto original_node_1 = column_reference.original_node();
    //             if (original_node_1->type == LQPNodeType::StoredTable) {
    //               const auto stored_table_node_1 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_1);
    //               table_name_1 = stored_table_node_1->table_name;
    //             }
    //           }
    //         }
    //       }
    //       const auto& perf_data = op->performance_data();

    //       auto table_id = _table_name_id_map.left.at(table_name_0);
    //       auto identifier = std::make_pair(table_id, original_column_id_0);
    //       update_map(join_map, identifier, perf_data);

    //       // How do we know whether the left_input_rows are actually added to the left table?
    //       table_id = _table_name_id_map.left.at(table_name_1);
    //       identifier = std::make_pair(table_id, original_column_id_1);
    //       update_map(join_map, identifier, perf_data, false);
    //     }
    //   }
    // }
}


void WorkloadPredictor::_process_index_scan(std::shared_ptr<const AbstractOperator> op, size_t frequency) {
  const auto node = op->lqp_node();
  const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);
  const auto operator_predicates = OperatorScanPredicate::from_expression(*predicate_node->predicate(), *node);
  
  if (operator_predicates->size() < 2) {
    for (const auto& el : node->node_expressions) {
      visit_expression(el, [&](const auto& expression) {
        if (expression->type == ExpressionType::LQPColumn) {
          const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
          const auto column_reference = column_expression->column_reference;
          const auto original_node = column_reference.original_node();

          if (original_node->type == LQPNodeType::StoredTable) {
            const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            const auto& table_name = stored_table_node->table_name;
            // const auto table_id = _table_name_id_map.left.at(table_name);

            const auto original_column_id = column_reference.original_column_id();
            const auto identifier = TableColumnIdentifier(table_name, original_column_id);
            const auto& perf_data = op->performance_data();

            // ToDo: This is a hack to only get the columns that can actually be indexed with the current rule. Change with better index rule
            if (original_node == node->left_input()) {
              const auto scan_access = ScanAccess(frequency * perf_data.walltime, perf_data.timestamp, frequency * perf_data.input_rows_left, frequency * perf_data.output_rows);
              _last_workload.add_access(identifier, scan_access);
            }
          }
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
  }
}

void WorkloadPredictor::_process_table_scan(std::shared_ptr<const AbstractOperator> op, size_t frequency) {
  const auto node = op->lqp_node();
  const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);
  const auto operator_predicates = OperatorScanPredicate::from_expression(*predicate_node->predicate(), *node);
  
  if (operator_predicates->size() < 2) {
    for (const auto& el : node->node_expressions) {
      visit_expression(el, [&](const auto& expression) {
        if (expression->type == ExpressionType::LQPColumn) {
          const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
          const auto column_reference = column_expression->column_reference;
          const auto original_node = column_reference.original_node();

          if (original_node->type == LQPNodeType::StoredTable) {
            const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            const auto& table_name = stored_table_node->table_name;
            // const auto table_id = _table_name_id_map.left.at(table_name);

            const auto original_column_id = column_reference.original_column_id();
            const auto identifier = TableColumnIdentifier(table_name, original_column_id);
            const auto& perf_data = op->performance_data();

            // ToDo: This is a hack to only get the columns that can actually be indexed with the current rule. Change with better index rule
            if (original_node == node->left_input()) {
              // std::cout << identifier << " " << std::to_string(perf_data.input_rows_left) << std::endl;
              const auto scan_access = ScanAccess(frequency * perf_data.walltime, perf_data.timestamp, frequency * perf_data.input_rows_left, frequency * perf_data.output_rows);
              _last_workload.add_access(identifier, scan_access);
            }
          }
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
  }
}

void WorkloadPredictor::_process_pqp(std::shared_ptr<const AbstractOperator> op, size_t frequency) {
  // TODO: handle diamonds
  // Todo: handle index scans/joins
  // TODO frequency should be used here not in the methods themselves
  if (op->type() == OperatorType::TableScan) {
    _process_table_scan(op, frequency);
  } else if (op->type() == OperatorType::JoinHash || op->type() == OperatorType::JoinMPSM ||
             op->type() == OperatorType::JoinNestedLoop || op->type() == OperatorType::JoinSortMerge) {
    _process_join(op, frequency);
  } else if (op->type() == OperatorType::IndexScan) {
    _process_index_scan(op, frequency);
  } else {
  }

  if (op->input_left()) _process_pqp(op->input_left(), frequency);
  if (op->input_right()) _process_pqp(op->input_right(), frequency);
}

// This first workload predictor version forecasts that the future workload will exactly look like the previous one
const Workload WorkloadPredictor::_calculate_forecast() const {
  const auto forecasted_workload = _last_workload;

  return forecasted_workload;
}

const Workload WorkloadPredictor::get_forecasts() {
  // _update_table_metadata();
  _last_workload = Workload();

  for (const auto& [query_string, physical_query_plan] : SQLPhysicalPlanCache::get()) {
    size_t frequency = SQLPhysicalPlanCache::get().get_frequency(query_string);
    _process_pqp(physical_query_plan, frequency);
    // physical_query_plan->print(std::cout);
  }

  const auto forecasted_workload = _calculate_forecast();

  return forecasted_workload;
}

// void WorkloadPredictor::_update_table_metadata() {
//   // ToDo: Clear
//   // ToDo: Step 2 Don't clear, but add new

//   uint16_t next_table_id = 0;
//   for (const auto& table_name : _sm.table_names()) {
//     _table_name_id_map.insert(table_name_id(table_name, next_table_id));

//     auto next_attribute_id = 0;
//     const auto table = StorageManager::get().get_table(table_name);
//     for (const auto& column_def : table->column_definitions()) {
//       const auto& column_name = column_def.name;
//       const auto identifier = std::make_pair(table_name, next_attribute_id++);
//       _attribute_id_name_map.emplace(identifier, column_name);
//     }

//     ++next_table_id;
//   }
// }

}  // namespace opossum
