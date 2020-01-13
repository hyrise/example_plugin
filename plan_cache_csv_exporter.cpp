#include <fstream>

#include "plan_cache_csv_exporter.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/table.hpp"

namespace opossum {

PlanCacheCsvExporter::PlanCacheCsvExporter() : _sm{Hyrise::get().storage_manager} {
  std::ofstream table_scans_csv;
  std::ofstream joins_csv;
  std::ofstream validates_csv;
  std::ofstream aggregates_csv;
  std::ofstream projections_csv;

  table_scans_csv.open("table_scans.csv");
  joins_csv.open("joins.csv");
  validates_csv.open("validates.csv");
  aggregates_csv.open("aggregates.csv");
  projections_csv.open("projections.csv");

  table_scans_csv << "QUERY_HASH,SCAN_TYPE,TABLE_NAME,COLUMN_NAME,INPUT_ROWS,OUTPUT_ROWS,RUNTIME_NS,DESCRIPTION\n";
  joins_csv << "QUERY_HASH,JOIN_MODE,LEFT_TABLE_NAME,LEFT_COLUMN_NAME,LEFT_TABLE_ROW_COUNT,RIGHT_TABLE_NAME,RIGHT_COLUMN_NAME,RIGHT_TABLE_ROW_COUNT,OUTPUT_ROWS,PREDICATE_COUNT,PRIMARY_PREDICATE,RUNTIME_NS\n";
  validates_csv << "QUERY_HASH,INPUT_ROWS,OUTPUT_ROWS,RUNTIME_NS\n";
  aggregates_csv << "QUERY_HASH,AGGREGATE_HASH,COLUMN_TYPE,TABLE_NAME,COLUMN_NAME,GROUP_BY_COLUMN_COUNT,AGGREGATE_COLUMN_COUNT,INPUT_ROWS,OUTPUT_ROWS,RUNTIME_NS,DESCRIPTION\n";
  projections_csv << "QUERY_HASH,PROJECTION_HASH,COLUMN_TYPE,TABLE_NAME,COLUMN_NAME,INPUT_ROWS,OUTPUT_ROWS,RUNTIME_NS,DESCRIPTION\n";

  table_scans_csv.close();
  joins_csv.close();
  validates_csv.close();
  aggregates_csv.close();
  projections_csv.close();


  for (const auto& [query_string, physical_query_plan] : *SQLPipelineBuilder::default_pqp_cache) {
    std::stringstream query_hex_hash;
    query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

    _process_pqp(physical_query_plan, query_hex_hash.str());
    // physical_query_plan->print(std::cout);
  }
}

std::string PlanCacheCsvExporter::_process_join(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash) {
  const auto node = op->lqp_node;
  const auto join_node = std::dynamic_pointer_cast<const JoinNode>(node);

  const auto operator_predicate = OperatorJoinPredicate::from_expression(*(join_node->node_expressions[0]),
                                                                         *node->left_input(), *node->right_input());

  std::stringstream ss;
  ss << query_hex_hash << "," << join_mode_to_string.left.at(join_node->join_mode) << ",";
  if (operator_predicate.has_value()) {
    const auto column_expressions = join_node->column_expressions();
    const auto predicate_expression = std::dynamic_pointer_cast<const AbstractPredicateExpression>(join_node->node_expressions[0]);
    std::string table_name_0, table_name_1;
    ColumnID original_column_id_0, original_column_id_1;

      // if (*column_expression == *(predicate_expression->arguments[0])) {
      {
        const auto column_expression = predicate_expression->arguments[0];
        if (column_expression->type == ExpressionType::LQPColumn) {
          const auto column_reference = std::dynamic_pointer_cast<LQPColumnExpression>(column_expression)->column_reference;
          original_column_id_0 = column_reference.original_column_id();

          const auto original_node_0 = column_reference.original_node();
          if (original_node_0->type == LQPNodeType::StoredTable) {
            const auto stored_table_node_0 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_0);
            table_name_0 = stored_table_node_0->table_name;
          }
        }
      }
      // }

      // if (*column_expression == *(predicate_expression->arguments[1])) {
      {
        const auto column_expression = predicate_expression->arguments[1];
        if (column_expression->type == ExpressionType::LQPColumn) {
          const auto column_reference = std::dynamic_pointer_cast<LQPColumnExpression>(column_expression)->column_reference;
          original_column_id_1 = column_reference.original_column_id();
          
          const auto original_node_1 = column_reference.original_node();
          if (original_node_1->type == LQPNodeType::StoredTable) {
            const auto stored_table_node_1 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_1);
            table_name_1 = stored_table_node_1->table_name;
          }
        }
      }
      // }
      const auto& perf_data = op->performance_data;

      std::string column_name_0, column_name_1;
      // In cases where the join partner is not a column, we fall back to empty column names.
      // Exemplary query is the rewrite of TPC-H Q2 where `min(ps_supplycost)` is joined with `ps_supplycost`.
      if (table_name_0 != "") {
        const auto sm_table_0 = _sm.get_table(table_name_0);
        column_name_0 = sm_table_0->column_names()[original_column_id_0];
      }
      if (table_name_1 != "") {
        const auto sm_table_1 = _sm.get_table(table_name_1);
        column_name_1 = sm_table_1->column_names()[original_column_id_1];
      }

      // auto table_id = _table_name_id_map.left.at(table_name_0);
      // auto identifier = std::make_pair(table_id, original_column_id_0);
      ss << table_name_0 << "," << column_name_0 << "," << *(perf_data->input_row_count_left)  << ",";
      ss << table_name_1 << "," << column_name_1 << "," << *(perf_data->input_row_count_right) << ",";
      ss << *perf_data->output_row_count << ",";
      ss << join_node->node_expressions.size() << "," << predicate_condition_to_string.left.at((*operator_predicate).predicate_condition) << ",";
      ss << perf_data->walltime.count() << "\n";
      // update_map(join_map, identifier, perf_data);

      // How do we know whether the left_input_rows are actually added to the left table?
      // table_id = _table_name_id_map.left.at(table_name_1);
      // identifier = std::make_pair(table_id, original_column_id_1);
      // update_map(join_map, identifier, perf_data, false);

  } else {
    ss << "UNEXPECTED join operator_predicate.has_value()";
    std::cout << op << std::endl;
  }

  return ss.str();
}


void PlanCacheCsvExporter::_process_index_scan(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash) {
  const auto node = op->lqp_node;
  const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);
  const auto operator_predicates = OperatorScanPredicate::from_expression(*predicate_node->predicate(), *node);

  // TODO(anyone): I never used an index scan, so I am not sure if I am handling it right.
  std::cout << "Unhandled operation ..." << std::endl;
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

            const auto& perf_data = op->performance_data;
            std::cout << table_name << "," << *(perf_data->input_row_count_left) << std::endl;
          }
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
  }
}

std::string PlanCacheCsvExporter::_process_table_scan(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash) {
  std::stringstream ss;

  const auto node = op->lqp_node;
  const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);

  const auto predicate = predicate_node->predicate();
  // We iterate through the expression until we find the desired column being scanned.This works acceptably ok for most
  // scans we are interested in (e.g., visits both columns of a column vs column scan).
  visit_expression(predicate, [&](const auto& expression) {
    if (expression->type == ExpressionType::LQPColumn) {
      const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      const auto column_reference = column_expression->column_reference;
      const auto original_node = column_reference.original_node();

      // Check if scan on data or reference table (this should ignore scans of temporary columns)
      if (original_node->type == LQPNodeType::StoredTable) {
        ss << query_hex_hash << ",";
        if (original_node == node->left_input()) {
          ss << "COLUMN_SCAN,";
        } else {
          ss << "REFERENCE_SCAN,";
        }

        const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
        const auto& table_name = stored_table_node->table_name;
        ss << table_name << ",";

        const auto original_column_id = column_reference.original_column_id();
        const auto& perf_data = op->performance_data;
        auto description = op->description();
        description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
        description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

        const auto sm_table = _sm.get_table(table_name);
        ss << sm_table->column_names()[original_column_id] << "," << *(perf_data->input_row_count_left) << ",";
        ss << *perf_data->output_row_count << ",";
        ss << perf_data->walltime.count() << ",\"";
        ss << description << "\"\n";
      }
    }
    return ExpressionVisitation::VisitArguments;
  });

  return ss.str();
}

std::string PlanCacheCsvExporter::_process_validate(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash) {
  const auto& perf_data = op->performance_data;

  std::stringstream ss;
  ss << query_hex_hash << "," << *(perf_data->input_row_count_left) << "," << *perf_data->output_row_count << "," << perf_data->walltime.count() << "\n";

  return ss.str();
}

std::string PlanCacheCsvExporter::_process_aggregate(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash) {
  const auto node = op->lqp_node;
  const auto aggregate_node = std::dynamic_pointer_cast<const AggregateNode>(node);

  std::stringstream ss;
  std::ostringstream op_description_ostream;
  op_description_ostream << *op;
  std::stringstream agg_hex_hash;
  agg_hex_hash << std::hex << std::hash<std::string>{}(op_description_ostream.str());

  std::cout << "TODO: handle group bys and expressions." << std::endl;

  for (const auto& el : aggregate_node->node_expressions) {
    // TODO: ensure we do not traverse too deep here, isn't the loop sufficient?
    visit_expression(el, [&](const auto& expression) {
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
        const auto column_reference = column_expression->column_reference;
        const auto original_node = column_reference.original_node();

        if (original_node->type == LQPNodeType::StoredTable) {
          ss << query_hex_hash << "," << agg_hex_hash.str() << ",";
          if (original_node == node->left_input()) {
            ss << "COLUMN_AGGREGATE,";
          } else {
            ss << "REFERENCE_AGGREGATE,";
          }

          const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
          const auto& table_name = stored_table_node->table_name;
          ss << table_name << ",";

          const auto original_column_id = column_reference.original_column_id();
          const auto& perf_data = op->performance_data;

          const auto node_expression_count = aggregate_node->node_expressions.size();
          const auto group_by_column_count = aggregate_node->aggregate_expressions_begin_idx;
          ss << group_by_column_count << "," << (node_expression_count - group_by_column_count) << ",";

          const auto sm_table = _sm.get_table(table_name);
          ss << sm_table->column_names()[original_column_id] << "," << *(perf_data->input_row_count_left) << ",";
          ss << *perf_data->output_row_count << ",";
          ss << perf_data->walltime.count() << ",\"";
          ss << op->description() << "\"\n";
        }
      }
      return ExpressionVisitation::VisitArguments;
    });
  }

  return ss.str();
}

std::string PlanCacheCsvExporter::_process_projection(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash) {
  const auto node = op->lqp_node;
  const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(node);

  std::string output;
  std::ostringstream op_description_ostream;
  op_description_ostream << *op;
  std::stringstream proj_hex_hash;
  proj_hex_hash << std::hex << std::hash<std::string>{}(op_description_ostream.str());
  const auto proj_hex_hash_str = proj_hex_hash.str();

  for (const auto& el : projection_node->node_expressions) {
    std::vector<std::string> stringstreams(projection_node->node_expressions.size()*100);
    auto write_id = std::atomic<size_t>(0);
    visit_expression(el, [&](const auto& expression) {
      const auto visit_write_id = ++write_id;
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
        const auto column_reference = column_expression->column_reference;
        const auto original_node = column_reference.original_node();

        if (original_node->type == LQPNodeType::StoredTable) {
          const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
          const auto& table_name = stored_table_node->table_name;
          const auto original_column_id = column_reference.original_column_id();
          const auto& perf_data = op->performance_data;
          auto description = op->lqp_node->description();
          description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
          description.erase(std::remove(description.begin(), description.end(), '"'), description.end());
          const auto sm_table = _sm.get_table(table_name);
          const auto column_name = sm_table->column_names()[original_column_id];
          const auto output_rows = *perf_data->output_row_count;
          const auto walltime = perf_data->walltime.count();

          stringstreams[visit_write_id] += query_hex_hash + "," + proj_hex_hash_str;
          if (original_node == node->left_input()) {
            stringstreams[visit_write_id] += ",COLUMN_PROJECTION,";
          } else {
            stringstreams[visit_write_id] += ",REFERENCE_PROJECTION,";
          }
          stringstreams[visit_write_id] += table_name + ",";
          stringstreams[visit_write_id] += column_name + "," + std::to_string(*(perf_data->input_row_count_left)) + ",";
          stringstreams[visit_write_id] += std::to_string(output_rows) + ",";
          stringstreams[visit_write_id] += std::to_string(walltime) + ",\"";
          stringstreams[visit_write_id] += description + "\"";
        }
      }
      // else {
      //   if (expression->type == ExpressionType::Function) {
      //     std::cout << "Function " << expression->as_column_name() << " - " << expression->arguments.size() << std::endl;
      //     for (const auto& t : expression->arguments) {
      //       if (t->type == ExpressionType::LQPColumn) {
      //         std::cout << "#" << t->as_column_name() << std::endl;
      //       }
      //     }
      //   }
      //   if (expression->type == ExpressionType::Arithmetic) {
      //     std::cout << "Arithmetic " << expression->as_column_name() << " - " << expression->arguments.size() << std::endl;
      //     for (const auto& t : expression->arguments) {
      //       if (t->type == ExpressionType::LQPColumn) {
      //         std::cout << "#" << t->as_column_name() << std::endl;
      //       }
      //     }
      //   }
      //   stringstreams[visit_write_id] += query_hex_hash + "," + proj_hex_hash_str;
      //   stringstreams[visit_write_id] += ",CALC_PROJECTION,";
      //   stringstreams[visit_write_id] += ",,,,,\"[Projection] Calculation: ";
      //   stringstreams[visit_write_id] += expression->as_column_name() + "\"";
      // }
      return ExpressionVisitation::VisitArguments;
    });
    
    for (const auto& stringstream : stringstreams) {
      if (stringstream.size() > 0) {
        output += stringstream + "\n";
      }
    }
  }

  return output;
}


void PlanCacheCsvExporter::_process_pqp(std::shared_ptr<const AbstractOperator> op, const std::string query_hex_hash) {
  std::ofstream table_scans_csv;
  std::ofstream joins_csv;
  std::ofstream validates_csv;
  std::ofstream aggregates_csv;
  std::ofstream projections_csv;

  table_scans_csv.open("table_scans.csv", std::ios_base::app);
  joins_csv.open("joins.csv", std::ios_base::app);
  validates_csv.open("validates.csv", std::ios_base::app);
  aggregates_csv.open("aggregates.csv", std::ios_base::app);
  projections_csv.open("projections.csv", std::ios_base::app);

  // TODO(anyone): handle diamonds?
  // Todo: handle index scans/joins
  // TODO frequency should be used here not in the methods themselves
  if (op->type() == OperatorType::TableScan) {
    table_scans_csv << _process_table_scan(op, query_hex_hash);
  } else if (op->type() == OperatorType::JoinHash || op->type() == OperatorType::JoinNestedLoop || op->type() == OperatorType::JoinSortMerge) {
    joins_csv << _process_join(op, query_hex_hash);
  } else if (op->type() == OperatorType::IndexScan) {
    _process_index_scan(op, query_hex_hash);
  } else if (op->type() == OperatorType::Validate) {
    validates_csv << _process_validate(op, query_hex_hash);
  } else if (op->type() == OperatorType::Aggregate) {
    aggregates_csv << _process_aggregate(op, query_hex_hash);
  } else if (op->type() == OperatorType::Projection) {
    projections_csv << _process_projection(op, query_hex_hash);
  } else {
  }

  if (op->input_left()) _process_pqp(op->input_left(), query_hex_hash);
  if (op->input_right()) _process_pqp(op->input_right(), query_hex_hash);
}

}  // namespace opossum
