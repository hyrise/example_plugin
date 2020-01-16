#include <fstream>
#include <unordered_set>

#include "driver.hpp"
#include "plan_cache_csv_exporter.hpp"

#include "hyrise.hpp"
#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"
#define MACOS OS
#include "tpcds/tpcds_table_generator.hpp"

using namespace opossum;  // NOLINT

namespace {

// Shamelessly copied from tpcds_benchmark.cpp
const std::unordered_set<std::string> filename_blacklist() {
  auto filename_blacklist = std::unordered_set<std::string>{};
  const auto blacklist_file_path = "hyrise/resources/benchmark/tpcds/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    std::string filename;
    while (std::getline(blacklist_file, filename)) {
      if (filename.size() > 0 && filename.at(0) != '#') {
        filename_blacklist.emplace(filename);
      }
    }
    blacklist_file.close();
  }
  return filename_blacklist;
}

// void extract_meta_data(std::string folder_name) {
//   std::ofstream table_meta_data_csv_file(folder_name + "/table_meta_data.csv");
//   table_meta_data_csv_file << "TABLE_NAME,ROW_COUNT,MAX_CHUNK_SIZE\n";

//   std::ofstream attribute_meta_data_csv_file(folder_name + "/attribute_meta_data.csv");
//   attribute_meta_data_csv_file << "TABLE_NAME,COLUMN_NAME,DATA_TYPE,DISTINCT_VALUE_COUNT,IS_NULLABLE\n";

//   std::ofstream segment_meta_data_csv_file(folder_name + "/segment_meta_data.csv");
//   segment_meta_data_csv_file << "TABLE_NAME,COLUMN_NAME,CHUNK_ID,ENCODING,COMPRESSION,ROW_COUNT,SIZE_IN_BYTES\n";

//   auto &sm = Hyrise::get().storage_manager;

//   for (const auto& table_name : sm.table_names()) {
//     const auto& table = sm.get_table(table_name);

//     table_meta_data_csv_file << table_name << "," << table->row_count() << ","
//                              << table->max_chunk_size() << std::endl;

//     for (const auto& column_def : table->column_definitions()) {
//       const auto& column_name = column_def.name;

//       // TODO(Bouncner): get distinct count via histogram as soon as we have merged the current master
//       attribute_meta_data_csv_file << table_name << "," << column_name << ","
//                                    << data_type_to_string.left.at(column_def.data_type) << ",100,"
//                                    << (column_def.nullable ? "TRUE" : "FALSE") << "\n";

//       const auto chunk_count = table->chunk_count();
//       for (auto chunk_id = ChunkID{0}, end = chunk_count; chunk_id < end; ++chunk_id) {
//         const auto& chunk = table->get_chunk(chunk_id);
//         const auto column_id = table->column_id_by_name(column_name);
//         const auto& segment = chunk->get_segment(column_id);

//         const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
//         const auto encoding_type = encoded_segment->encoding_type();

//         segment_meta_data_csv_file << table_name << "," << column_name << "," << chunk_id << "," << encoding_type_to_string.left.at(encoding_type) << ",";

//         if (encoded_segment->compressed_vector_type()) {
//           switch (*encoded_segment->compressed_vector_type()) {
//             case CompressedVectorType::FixedSize4ByteAligned: {
//               segment_meta_data_csv_file << "FixedSize4ByteAligned";
//               break;
//             }
//             case CompressedVectorType::FixedSize2ByteAligned: {
//               segment_meta_data_csv_file << "FixedSize2ByteAligned";
//               break;
//             }
//             case CompressedVectorType::FixedSize1ByteAligned: {
//               segment_meta_data_csv_file << "FixedSize1ByteAligned";
//               break;
//             }
//             case CompressedVectorType::SimdBp128: {
//               segment_meta_data_csv_file << "SimdBp128";
//               break;
//             }
//             default:
//               segment_meta_data_csv_file << "NONE";
//           }
//         }

//         segment_meta_data_csv_file << "," << segment->size() << "," << encoded_segment->memory_usage(MemoryUsageCalculationMode::Sampled) << "\n";
//       }
//     }
//   }

//   table_meta_data_csv_file.close();
//   attribute_meta_data_csv_file.close();
//   segment_meta_data_csv_file.close();
// }

// void extract_physical_query_plan_cache_data(std::string folder_name) {
//   std::ofstream plan_cache_csv_file(folder_name + "/plan_cache.csv");
//   plan_cache_csv_file << "QUERY_HASH,EXECUTION_COUNT,QUERY_STRING\n";

//   for (const auto& [query_string, physical_query_plan] : *SQLPipelineBuilder::default_pqp_cache) {
//     auto& gdfs_cache = dynamic_cast<GDFSCache<std::string, std::shared_ptr<AbstractOperator>>&>(SQLPipelineBuilder::default_pqp_cache->cache());
//     const size_t frequency = gdfs_cache.frequency(query_string);

//     std::stringstream query_hex_hash;
//     query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

//     auto query_single_line(query_string);
//     query_single_line.erase(std::remove(query_single_line.begin(), query_single_line.end(), '\n'),
//                             query_single_line.end());

//     plan_cache_csv_file << query_hex_hash.str() << "," << frequency << ",\"" << query_single_line << "\"\n";
//   }

//   plan_cache_csv_file.close();
// }

}  // namespace

const std::string Driver::description() const { return "This driver executes benchmarks and outputs its plan cache to an array of CSV files."; }

void Driver::start() {
  const auto BENCHMARKS = std::vector<std::string>{"TPC-H", "TPC-DS", "JOB"}; 

  const auto env_var = std::getenv("BENCHMARK_TO_RUN");
  if (env_var == NULL) {
    std::cerr << "Please pass environment variable \"BENCHMARK_TO_RUN\" to set a target benchmark.\nExiting Plugin." << std::flush;
  	return;
  } 

  auto BENCHMARK = std::string(env_var);
  if (std::find(BENCHMARKS.begin(), BENCHMARKS.end(), BENCHMARK) == BENCHMARKS.end()) {
  	std::cerr << "Benchmark \"" << BENCHMARK << "\" not supported. Supported benchmarks: ";
  	for (const auto& benchmark : BENCHMARKS) std::cout << "\"" << benchmark << "\" ";
  	std::cerr << "\nExiting." << std::flush;
  	return;
  }
  std::cout << "Running " << BENCHMARK << " ... " << std::endl;

  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  config->max_runs = 10;
  config->enable_visualization = false;
  config->chunk_size = 100'000;
  config->cache_binary_tables = true;

  constexpr auto USE_PREPARED_STATEMENTS = false;
  auto SCALE_FACTOR = 17.0f;  // later overwritten


  //
  //  TPC-H
  //
  if (BENCHMARK == "TPC-H") {
    SCALE_FACTOR = 0.01f;
    // const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{5}};
    // auto item_runner = std::make_ unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR, tpch_query_ids_benchmark);
    auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(
        *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, config), BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }
  //
  //  /TPC-H
  //


  //
  //  TPC-DS
  //
  else if (BENCHMARK == "TPC-DS") {
    SCALE_FACTOR = 1.0f;
    config->max_runs = 1;
    const std::string query_path = "hyrise/resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

    auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, filename_blacklist());
    auto table_generator = std::make_unique<TpcdsTableGenerator>(SCALE_FACTOR, config);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(query_generator), std::move(table_generator),
                                                              opossum::BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }
  //
  //  /TPC-DS
  //

  //
  //  JOB
  //
  else if (BENCHMARK == "JOB") {
    config->max_runs = 1;

    const auto table_path = "hyrise/imdb_data";
    const auto query_path = "hyrise/third_party/join-order-benchmark";
    const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

    auto benchmark_item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, non_query_file_names);
    auto table_generator = std::make_unique<FileBasedTableGenerator>(config, table_path);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(benchmark_item_runner), std::move(table_generator),
                                                              BenchmarkRunner::create_context(*config));

    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }
  //
  //  /JOB
  //


  // std::string folder_name = std::string(BENCHMARK) + "__" + std::to_string(SCALE_FACTOR);
  // std::filesystem::create_directories(folder_name);

  // // TODO: second function to exportCSV and first to new plugin
  // extract_meta_data(folder_name);
  // extract_physical_query_plan_cache_data(folder_name);

  if (SQLPipelineBuilder::default_pqp_cache->size() > 0) {
  	std::cout << "Exporting plan cache ... ";
    PlanCacheCsvExporter();
    std::cout << "done." << std::endl;
  } else {
    std::cout << "Plan cache is empty." << std::endl;
  }

  // std::cout << "Please move workload files to generated folder " << folder_name << "." << std::endl;


}

void Driver::stop() {
}

EXPORT_PLUGIN(Driver)
