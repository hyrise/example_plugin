#include <fstream>

#include "plan_cache_csv_exporter.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/table_scan.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/table.hpp"

namespace {

using namespace opossum;  // NOLINT

std::string wrap_string(const std::string str) {
  return std::string{"\""} + str + "\"";
}

void add_to_map(const std::map<std::string, std::string>& operator_instance,
                std::map<std::string, std::vector<std::string>>& operator_instances) {
  // for the first entry, create all vectors and reserve
  if (operator_instances.empty()) {
    for (const auto& [key, value] : operator_instance) {
      operator_instances[key].reserve(100);
    }  
  }
  for (const auto& [key, value] : operator_instance) {
    Assert(operator_instances.contains(key), "Unexpected key added to operator_instances: " + key);
    operator_instances[key].emplace_back(value);
  }
}

std::string camel_to_csv_row_title(const std::string& title) {
  auto result = std::string{};
  auto string_index = size_t{0};
  for (unsigned char character : title) {
    if (string_index > 0 && std::isupper(character)) {
      result += '_';
      result += character;
      ++string_index;
      continue;
    }
    result += std::toupper(character);
    ++string_index;
  }
  return result;
}

std::string get_operator_hash(const std::shared_ptr<const AbstractOperator> op) {
  std::stringstream ss;
  std::ostringstream op_description_ostream;
  op_description_ostream << *op;
  std::stringstream description_hex_hash;
  description_hex_hash << std::hex << std::hash<std::string>{}(op_description_ostream.str());
  ss << op << description_hex_hash.str();

  return ss.str();
}

}  // namespace

namespace opossum {

PlanCacheCsvExporter::PlanCacheCsvExporter(const std::string export_folder_name,
                                           const std::shared_ptr<SQLPhysicalPlanCache> plan_cache)
    : _sm{Hyrise::get().storage_manager}, _plan_cache{plan_cache}, _export_folder_name{export_folder_name}, _table_scans{}, _projections{} {
  std::ofstream joins_csv;
  std::ofstream general_operators_csv;

  joins_csv.open(_export_folder_name + "/joins.csv");
  general_operators_csv.open(_export_folder_name + "/general_operators.csv");

  joins_csv << "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|JOIN_MODE|LEFT_TABLE_NAME|LEFT_COLUMN_NAME|LEFT_TABLE_CHUNK_COUNT|LEFT_TABLE_ROW_COUNT|LEFT_COLUMN_TYPE|RIGHT_TABLE_NAME|RIGHT_COLUMN_NAME|RIGHT_TABLE_CHUNK_COUNT|RIGHT_TABLE_ROW_COUNT|RIGHT_COLUMN_TYPE|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|PREDICATE_COUNT|PRIMARY_PREDICATE|IS_FLIPPED|RADIX_BITS|";
  for (const auto step_name : magic_enum::enum_names<JoinHash::OperatorSteps>()) {
    const auto step_name_str = std::string{step_name};
    joins_csv << camel_to_csv_row_title(step_name_str) << "_NS|";
  }
  joins_csv << "RUNTIME_NS|DESCRIPTION\n";
  general_operators_csv << "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|INPUT_CHUNK_COUNT|INPUT_ROW_COUNT|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|RUNTIME_NS\n";
  // aggregates_csv << "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|COLUMN_TYPE|TABLE_NAME|COLUMN_NAME|GROUP_BY_COLUMN_COUNT|AGGREGATE_COLUMN_COUNT|INPUT_CHUNK_COUNT|INPUT_ROW_COUNT|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|";
  // for (const auto step_name : magic_enum::enum_names<AggregateHash::OperatorSteps>()) {
  //   const auto step_name_str = std::string{step_name};
  //   aggregates_csv << camel_to_csv_row_title(step_name_str) << "_NS|";
  // }
  // aggregates_csv << "RUNTIME_NS|DESCRIPTION\n";

  joins_csv.close();
  general_operators_csv.close();
  // aggregates_csv.close();
}

void PlanCacheCsvExporter::run() {
  std::ofstream plan_cache_csv_file(_export_folder_name + "/plan_cache.csv");
  plan_cache_csv_file << "QUERY_HASH|EXECUTION_COUNT|QUERY_STRING\n";

  const auto cache_snapshot = _plan_cache->snapshot();
  for (const auto& [query_string, snapshot_entry] : cache_snapshot) {
    const auto& physical_query_plan = snapshot_entry.value;
    const auto& frequency = snapshot_entry.frequency;
    Assert(frequency, "Optional frequency is unexpectedly not set.");

    std::stringstream query_hex_hash;
    query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

    std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_pqp_nodes;
    _process_pqp(physical_query_plan, query_hex_hash.str(), visited_pqp_nodes);

    auto query_single_line{query_string};
    std::replace(query_single_line.begin(), query_single_line.end(), '\n', ' ');
    plan_cache_csv_file << "\"" << query_hex_hash.str() << "\"" << "|" << *frequency << "|\"" << query_single_line << "\"\n";
  }

  write_to_disk();
  write_map_to_disk(_export_folder_name + "/aggregates.csv", _aggregates);
}

void PlanCacheCsvExporter::write_to_disk() const {
  auto write_lines = [](const auto file_name, const auto operator_information) {
    const auto separator = "|";

    std::ofstream output_csv;
    output_csv.open(file_name);
    output_csv << operator_information.csv_header() << "\n";
    for (const auto& instance : operator_information.instances) {
      const auto string_vector = instance.string_vector();
      for (auto index = size_t{0}; index < string_vector.size(); ++index) {
        output_csv << string_vector[index];
        if (index < (string_vector.size() - 1)) {
          output_csv << separator;
        }
      }
      output_csv << "\n";
    }
    output_csv.close();
  };

  write_lines(_export_folder_name + "/get_tables.csv", _get_tables);
  write_lines(_export_folder_name + "/table_scans.csv", _table_scans);
  write_lines(_export_folder_name + "/projections.csv", _projections);
}

void PlanCacheCsvExporter::write_map_to_disk(const std::string file_name, const std::map<std::string, std::vector<std::string>>& operator_instances) const {
  const auto separator = "|";

  std::ofstream output_csv;
  output_csv.open(file_name);

  // Write header and gather keys to later check for same length of all measurements.
  auto keys = std::vector<std::string>{};
  auto is_first = true;
  for (const auto& [key, _] : operator_instances) {
    if (!is_first) {
      output_csv << separator;
    }

    output_csv << key;
    is_first = false;
    keys.emplace_back(key);
  }
  output_csv << '\n';

  auto measurement_count = size_t{0};
  for (auto key_id = size_t{1}; key_id < operator_instances.size(); ++key_id) {
    const auto size = operator_instances.at(keys[key_id]).size();
    Assert(size == operator_instances.at(keys[key_id - 1]).size(),
           "Lenghts of measurements are not equal.");
    measurement_count = size;
  }

  // TODO: I am not happy with that solution. Having vectors is nice, but writing to CSV is pretty inefficient.
  is_first = true;
  for (auto row_id = size_t{0}; row_id < measurement_count; ++row_id) {
    for (const auto& key : keys) {
      if (!is_first) {
        output_csv << separator;
      }
      output_csv << operator_instances.at(key)[row_id];
      is_first = false;
    }
    is_first = true;
    output_csv << '\n';
  }
  output_csv.close();
}

std::string PlanCacheCsvExporter::_process_join(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  const auto node = op->lqp_node;
  const auto join_node = std::dynamic_pointer_cast<const JoinNode>(node);

  const auto operator_predicate = OperatorJoinPredicate::from_expression(*(join_node->node_expressions[0]),
                                                                         *node->left_input(), *node->right_input());

  std::stringstream ss;
  ss << "JOIN|" << query_hex_hash << "|" << get_operator_hash(op) << "|" << get_operator_hash(op->left_input()) << "|"
     << get_operator_hash(op->right_input()) << "|" << join_mode_to_string.left.at(join_node->join_mode) << "|";
  if (operator_predicate.has_value()) {
    const auto predicate_expression = std::dynamic_pointer_cast<const AbstractPredicateExpression>(join_node->node_expressions[0]);
    std::string left_table_name{};
    std::string right_table_name{};
    ColumnID left_original_column_id{};
    ColumnID right_original_column_id{};
    std::string left_column_type{};
    std::string right_column_type{};

    const auto first_predicate_expression = predicate_expression->arguments[0];
    if (first_predicate_expression->type == ExpressionType::LQPColumn) {
      const auto left_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(first_predicate_expression);
      left_original_column_id = left_column_expression->original_column_id;

      const auto original_node_0 = left_column_expression->original_node.lock();
      if (original_node_0->type == LQPNodeType::StoredTable) {
        const auto stored_table_node_0 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_0);
        left_table_name = stored_table_node_0->table_name;

        if (original_node_0 == node->left_input()) {
          left_column_type = "DATA";
        } else {
          left_column_type = "REFERENCE";
        }
      }
    }

    const auto second_predicate_expression = predicate_expression->arguments[1];
    if (second_predicate_expression->type == ExpressionType::LQPColumn) {
      const auto right_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(second_predicate_expression);
      right_original_column_id = right_column_expression->original_column_id;
      
      const auto original_node_1 = right_column_expression->original_node.lock();
      if (original_node_1->type == LQPNodeType::StoredTable) {
        const auto stored_table_node_1 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_1);
        right_table_name = stored_table_node_1->table_name;

        if (original_node_1 == node->right_input()) {
          right_column_type = "DATA";
        } else {
          right_column_type = "REFERENCE";
        }
      }
    }

    std::string column_name_0, column_name_1;
    // In cases where the join partner is not a column, we fall back to empty column names.
    // Exemplary query is the rewrite of TPC-H Q2 where `min(ps_supplycost)` is joined with `ps_supplycost`.
    if (left_table_name != "") {
      const auto sm_table_0 = _sm.get_table(left_table_name);
      column_name_0 = sm_table_0->column_names()[left_original_column_id];
    }
    if (right_table_name != "") {
      const auto sm_table_1 = _sm.get_table(right_table_name);
      column_name_1 = sm_table_1->column_names()[right_original_column_id];
    }

    auto description = op->description();
    description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
    description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

    const auto& left_input_perf_data = op->left_input()->performance_data;
    const auto& right_input_perf_data = op->right_input()->performance_data;

    // Check if the join predicate has been switched (hence, it differs between LQP and PQP) which is done when
    // table A and B are joined but the join predicate is "flipped" (e.g., b.x = a.x). The effect of flipping is that
    // the predicates are in the order (left/right) as the join input tables are.
    if (!operator_predicate->is_flipped()) {
      ss << left_table_name << "|" << column_name_0 << "|" << left_input_perf_data->output_chunk_count
         << "|" << left_input_perf_data->output_row_count << "|" << left_column_type << "|";
      ss << right_table_name << "|" << column_name_1 << "|" << right_input_perf_data->output_chunk_count
         << "|" << right_input_perf_data->output_row_count << "|" << right_column_type << "|";
    } else {
      ss << right_table_name << "|" << column_name_1 << "|" << left_input_perf_data->output_chunk_count
         << "|" << left_input_perf_data->output_row_count << "|" << right_column_type << "|";
      ss << left_table_name << "|" << column_name_0 << "|" << right_input_perf_data->output_chunk_count
         << "|" << right_input_perf_data->output_row_count << "|" << left_column_type << "|";
    }

    const auto& perf_data = op->performance_data;
    ss << perf_data->output_chunk_count << "|" << perf_data->output_row_count << "|";
    ss << join_node->node_expressions.size() << "|" << predicate_condition_to_string.left.at((*operator_predicate).predicate_condition) << "|";
    if (const auto join_hash_op = dynamic_pointer_cast<const JoinHash>(op)) {
      const auto& join_hash_perf_data = dynamic_cast<const JoinHash::PerformanceData&>(*join_hash_op->performance_data);
      const auto build_and_probe_side_flipped = join_hash_perf_data.left_input_is_build_side ? "FALSE" : "TRUE";
      ss << build_and_probe_side_flipped << "|" << join_hash_perf_data.radix_bits << "|";

      for (const auto step_name : magic_enum::enum_values<JoinHash::OperatorSteps>()) {
        ss << join_hash_perf_data.get_step_runtime(step_name).count() << "|";
      }
    } else {
      ss << "FALSE|NULL|";
      for (auto index = size_t{0}; index < magic_enum::enum_count<JoinHash::OperatorSteps>(); ++index) {
        ss << "NULL|";
      }
    }
    ss << perf_data->walltime.count() << "|" << description << "\n";
  } else {
    ss << "UNEXPECTED join operator_predicate.has_value()";
    std::cout << op << std::endl;
  }

  return ss.str();
}

void PlanCacheCsvExporter::_process_table_scan(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  std::vector<SingleTableScan> table_scans;

  const auto node = op->lqp_node;
  const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);

  const auto predicate = predicate_node->predicate();
  // We iterate through the expression until we find the desired column being scanned. This works acceptably ok
  // for most scans we are interested in (e.g., visits both columns of a column vs column scan).
  visit_expression(predicate, [&](const auto& expression) {
    std::string column_type{};
    if (expression->type == ExpressionType::LQPColumn) {
      const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      const auto original_node = column_expression->original_node.lock();

      // Check if scan on data or reference table (this should ignore scans of temporary columns)
      if (original_node->type == LQPNodeType::StoredTable) {
        
        if (original_node == node->left_input()) {
          column_type = "DATA";
        } else {
          column_type = "REFERENCE";
        }

        const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
        const auto& table_name = stored_table_node->table_name;

        auto scan_predicate_condition = std::string{"Unknown"};
        const auto operator_scan_predicates = OperatorScanPredicate::from_expression(*predicate, *stored_table_node);
        if (operator_scan_predicates->size() == 1) {
          const auto condition = (*operator_scan_predicates)[0].predicate_condition;
          scan_predicate_condition = magic_enum::enum_name(condition);
        }

        const auto original_column_id = column_expression->original_column_id;
        const auto sm_table = _sm.get_table(table_name);
        std::string column_name = "";
        if (original_column_id != INVALID_COLUMN_ID) {
          column_name = sm_table->column_names()[original_column_id];
        } else {
          column_name = "COUNT(*)";
        }

        const auto table_scan_op = dynamic_pointer_cast<const TableScan>(op);
        Assert(table_scan_op, "Unexpected non-table-scan operators");
        const auto& operator_perf_data = dynamic_cast<const TableScan::PerformanceData&>(*table_scan_op->performance_data);

        auto description = op->description();
        description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
        description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

        const auto& left_input_perf_data = op->left_input()->performance_data;

        table_scans.emplace_back(SingleTableScan{query_hex_hash, get_operator_hash(op),
                                                 get_operator_hash(op->left_input()), "NULL", column_type, table_name,
                                                 column_name, scan_predicate_condition,
                                                 static_cast<size_t>(-17), // removed due to work on export plugin
                                                 static_cast<size_t>(-17), // removed due to work on export plugin
                                                 left_input_perf_data->output_chunk_count,
                                                 left_input_perf_data->output_row_count,
                                                 operator_perf_data.output_chunk_count,
                                                 operator_perf_data.output_row_count,
                                                 static_cast<size_t>(operator_perf_data.walltime.count()),
                                                 description});
      }
    }
    return ExpressionVisitation::VisitArguments;
  });

  _table_scans.instances.insert(_table_scans.instances.end(), table_scans.begin(), table_scans.end());
}

void PlanCacheCsvExporter::_process_get_table(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  std::vector<SingleGetTable> get_tables;

  const auto get_table_op = dynamic_pointer_cast<const GetTable>(op);
  Assert(get_table_op, "Processing GetTable operator but another operator was passed.");

  auto description = op->description();
  description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
  description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

  const auto& perf_data = op->performance_data;

  get_tables.emplace_back(SingleGetTable{query_hex_hash, get_operator_hash(op), "NULL", "NULL",
                                          get_table_op->table_name(), get_table_op->pruned_chunk_ids().size(),
                                          get_table_op->pruned_column_ids().size(), perf_data->output_chunk_count,
                                          perf_data->output_row_count,
                                          static_cast<size_t>(perf_data->walltime.count()), description});

  _get_tables.instances.insert(_get_tables.instances.end(), get_tables.begin(), get_tables.end());
}

std::string PlanCacheCsvExporter::_process_general_operator(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  const auto& perf_data = op->performance_data;
  const auto& left_input_perf_data = op->left_input()->performance_data;

  // TODO(Martin): Do we need the table name?
  const auto type_name = std::string{magic_enum::enum_name(op->type())};
  std::stringstream ss;
  ss << camel_to_csv_row_title(type_name) << "|" << query_hex_hash << "|" << get_operator_hash(op) << "|" << get_operator_hash(op->left_input())
     << "|NULL|" << left_input_perf_data->output_chunk_count << "|" << left_input_perf_data->output_chunk_count
     << "|" << perf_data->output_chunk_count << "|" << perf_data->output_row_count << "|"
     << perf_data->walltime.count() << "\n";

  return ss.str();
}

void PlanCacheCsvExporter::_process_aggregate(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  // Starting with the left input, as the aggregate itself will always be materializing.
  const auto input_is_materialized = _operator_result_is_probably_materialized(op->left_input());

  const auto node = op->lqp_node;
  const auto aggregate_node = std::static_pointer_cast<const AggregateNode>(node);

  const auto node_expression_count = aggregate_node->node_expressions.size();
  const auto group_by_column_count = aggregate_node->aggregate_expressions_begin_idx;

  auto expression_id = size_t{0};
  for (const auto& el : aggregate_node->node_expressions) {
    const auto add_stored_column_access = [this, &node](const auto& expression, auto& instance) {
      const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      const auto original_node = column_expression->original_node.lock();

      if (original_node->type == LQPNodeType::StoredTable) {
        auto column_type = std::string{""};
        if (original_node == node->left_input()) {
          column_type = "DATA";
        } else {
          column_type = "REFERENCE";
        }

        const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
        const auto& table_name = stored_table_node->table_name;

        const auto original_column_id = column_expression->original_column_id;

        const auto sm_table = _sm.get_table(table_name);
        Assert(original_column_id != INVALID_COLUMN_ID, "Unexpected column id. Count(*) should not reach this point.");
        const auto column_name = sm_table->column_names()[original_column_id];

        instance["COLUMN_TYPE"] = wrap_string(column_type);
        instance["TABLE_NAME"] = wrap_string(table_name);
        instance["COLUMN_NAME"] = wrap_string(column_name);
      } else {
        Fail("Found unexpected: " + std::string{magic_enum::enum_name(original_node->type)});
      }
    };

    const auto is_group_by_column = expression_id < group_by_column_count;
    Assert(is_group_by_column || el->requires_computation(), "Unexpected aggregation columns that does not require computation.");

    const auto& perf_data = op->performance_data;
    auto operator_instance = std::map<std::string, std::string>{{"OPERATOR_TYPE", wrap_string("AGGREGATE")},
      {"QUERY_HASH", wrap_string(query_hex_hash)}, {"OPERATOR_HASH", wrap_string(get_operator_hash(op))},
      {"LEFT_INPUT_OPERATOR_HASH", wrap_string(get_operator_hash(op->left_input()))},
      {"RIGHT_INPUT_OPERATOR_HASH", "NULL"}, {"GROUP_BY_COLUMN_COUNT", std::to_string(group_by_column_count)},
      {"COLUMN_TYPE", wrap_string("DATA")}, {"TABLE_NAME", "NULL"}, {"COLUMN_NAME", "NULL"},
      {"AGGREGATE_COLUMN_COUNT", std::to_string(node_expression_count - group_by_column_count)},
      {"IS_GROUP_BY_COLUMN", std::to_string(is_group_by_column)},
      {"IS_COUNT_STAR", "0"},
      {"INPUT_CHUNK_COUNT", std::to_string(op->left_input()->performance_data->output_chunk_count)},
      {"INPUT_ROW_COUNT", std::to_string(op->left_input()->performance_data->output_row_count)},
      {"OUTPUT_CHUNK_COUNT", std::to_string(perf_data->output_chunk_count)},
      {"OUTPUT_ROW_COUNT", std::to_string(perf_data->output_row_count)},
      {"RUNTIME_NS", std::to_string(perf_data->walltime.count())}, {"DESCRIPTION", wrap_string(op->description())}};

    // Add performance counter steps to output
    if (const auto aggregate_hash_op = dynamic_pointer_cast<const AggregateHash>(op)) {
      const auto& operator_perf_data = dynamic_cast<const OperatorPerformanceData<AggregateHash::OperatorSteps>&>(*aggregate_hash_op->performance_data);
      for (const auto step_name : magic_enum::enum_values<AggregateHash::OperatorSteps>()) {
        const auto step_name_key = camel_to_csv_row_title(std::string{magic_enum::enum_name(step_name)}) + "_NS";
        operator_instance[step_name_key] = std::to_string(operator_perf_data.get_step_runtime(step_name).count());
      }
    } else {
      for (const auto step_name : magic_enum::enum_values<AggregateHash::OperatorSteps>()) {
        const auto step_name_key = camel_to_csv_row_title(std::string{magic_enum::enum_name(step_name)}) + "_NS";
        operator_instance[step_name_key] = "NULL";
      }
    }


    if (el->type == ExpressionType::LQPColumn && !input_is_materialized) {
      // The table has not been materialized before (e.g., in a projection) and the expression is an LQPColumn
      // (e.g., GROUP BY l_linenumber)
      add_stored_column_access(el, operator_instance);
      add_to_map(operator_instance, _aggregates);
    } else {
      // It is not a direct column reference. This part handles count(*), aggregations (e.g., SUM),
      // or functions (e.g., CASE), or accesses to a materialized table.
      Assert(el->type == ExpressionType::Aggregate || el->type == ExpressionType::Function || el->type == ExpressionType::Arithmetic ||
             (input_is_materialized && el->type == ExpressionType::LQPColumn),
             "Found an unexpected expression in an aggregate node: " + std::string{magic_enum::enum_name(el->type)});

      const auto aggregate_expression = std::static_pointer_cast<const AggregateExpression>(el);
      const auto is_count_star = AggregateExpression::is_count_star(*aggregate_expression);

      if (aggregate_expression && is_count_star) {
        operator_instance["IS_COUNT_STAR"] = "1";
      } else if (!input_is_materialized && aggregate_expression && aggregate_expression->argument()->type == ExpressionType::LQPColumn) {
        add_stored_column_access(aggregate_expression->argument(), operator_instance);
      }

      add_to_map(operator_instance, _aggregates);
    }

    ++expression_id;
  }
}

void PlanCacheCsvExporter::_process_projection(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  std::vector<SingleProjection> projections;

  const auto input_is_materialized = _operator_result_is_probably_materialized(op->left_input());

  const auto node = op->lqp_node;
  const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(node);

  const auto& perf_data = op->performance_data;
  const auto& left_input_perf_data = op->left_input()->performance_data;

  auto description = op->lqp_node->description();
  description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
  description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

  for (const auto& el : projection_node->node_expressions) {
    if (input_is_materialized) {
      projections.emplace_back(SingleProjection{query_hex_hash, get_operator_hash(op),
        get_operator_hash(op->left_input()), "NULL", "DATA", "NULL",
        "NULL", el->requires_computation(), left_input_perf_data->output_chunk_count, left_input_perf_data->output_row_count,
        perf_data->output_chunk_count, perf_data->output_row_count,
        static_cast<size_t>(perf_data->walltime.count()), description});      
    } else {
      visit_expression(el, [&](const auto& expression) {
        if (expression->type == ExpressionType::LQPColumn) {
          const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
          const auto original_node = column_expression->original_node.lock();

          if (original_node->type == LQPNodeType::StoredTable) {
            const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            const auto& table_name = stored_table_node->table_name;
          
            const auto original_column_id = column_expression->original_column_id;
            const std::string column_type = (original_node == node->left_input()) ? "DATA" : "REFERENCE";
          
            const auto sm_table = _sm.get_table(table_name);
            std::string column_name = "";
            if (original_column_id != INVALID_COLUMN_ID) {
              column_name = sm_table->column_names()[original_column_id];
            } else {
              column_name = "COUNT(*)";
            }

            projections.emplace_back(SingleProjection{query_hex_hash, get_operator_hash(op),
              get_operator_hash(op->left_input()), "NULL", column_type, table_name,
              column_name, el->requires_computation(), left_input_perf_data->output_chunk_count, left_input_perf_data->output_row_count,
              perf_data->output_chunk_count, perf_data->output_row_count,
              static_cast<size_t>(perf_data->walltime.count()), description});
          }
        }

        return ExpressionVisitation::VisitArguments;
      });
    }
  }

  _projections.instances.insert(_projections.instances.end(), projections.begin(), projections.end());
}


void PlanCacheCsvExporter::_process_pqp(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash,
                                        std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes) {
  std::ofstream joins_csv;
  std::ofstream general_operators_csv;

  joins_csv.open(_export_folder_name + "/joins.csv", std::ios_base::app);
  general_operators_csv.open(_export_folder_name + "/general_operators.csv", std::ios_base::app);

  // TODO(anyone): handle diamonds?
  // Todo: handle index scans
  if (op->type() == OperatorType::TableScan) {
    _process_table_scan(op, query_hex_hash);
  } else if (op->type() == OperatorType::GetTable) {
    _process_get_table(op, query_hex_hash);
  } else if (op->type() == OperatorType::JoinHash || op->type() == OperatorType::JoinNestedLoop || op->type() == OperatorType::JoinSortMerge) {
    joins_csv << _process_join(op, query_hex_hash);
  } else if (op->type() == OperatorType::Aggregate) {
    _process_aggregate(op, query_hex_hash);
  } else if (op->type() == OperatorType::Projection) {
    _process_projection(op, query_hex_hash);
  } else if (op->type() == OperatorType::CreateView || op->type() == OperatorType::DropView || op->type() == OperatorType::TableWrapper) {
    // Problems when hashing their descriptions.
  } else {
    general_operators_csv << _process_general_operator(op, query_hex_hash);
  }

  visited_pqp_nodes.insert(op);

  const auto left_input = op->left_input();
  const auto right_input = op->right_input();
  if (left_input && !visited_pqp_nodes.contains(left_input)) {
    _process_pqp(left_input, query_hex_hash, visited_pqp_nodes);
    visited_pqp_nodes.insert(std::move(left_input));
  }
  if (right_input && !visited_pqp_nodes.contains(right_input)) {
    _process_pqp(right_input, query_hex_hash, visited_pqp_nodes);
    visited_pqp_nodes.insert(std::move(right_input));
  }
}


// TODO: we might switch from a boolean return value to keep track of single columns, this might be especially
// important for joins along the way.
// Currently for this function to work, the left_input() of the actual operator needs to be passed.
bool PlanCacheCsvExporter::_operator_result_is_probably_materialized(const std::shared_ptr<const AbstractOperator>& op) {
  auto left_input_table_probably_materialized = false;
  auto right_input_table_probably_materialized = false;

  if (op->type() == OperatorType::Aggregate) {
    return true;
  }

  if (op->type() == OperatorType::Projection) {
    const auto node = op->lqp_node;
    const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(node);

    for (const auto& el : projection_node->node_expressions) {
      if (el->requires_computation()) {
        return true;
      }
    }
  }

  if (op->left_input()) {
    left_input_table_probably_materialized = _operator_result_is_probably_materialized(op->left_input());
  }

  if (op->right_input()) {
    right_input_table_probably_materialized = _operator_result_is_probably_materialized(op->right_input());
  }

  return left_input_table_probably_materialized || right_input_table_probably_materialized;
}

}  // namespace opossum
