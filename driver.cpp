#include <fstream>
#include <thread>
#include <unordered_set>

#include <boost/algorithm/string.hpp>

#include "driver.hpp"
#include "plan_cache_csv_exporter.hpp"

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "operators/print.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpcc/tpcc_benchmark_item_runner.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"
#define MACOS OS
#include "tpcds/tpcds_table_generator.hpp"
#include "utils/timer.hpp"

using namespace opossum;  // NOLINT
using namespace std::chrono_literals;

// We do not use the MVCC delete plugin for now, as we write by far to few lines in TPC-C to make a difference.
constexpr auto USE_MVCC_DELETE = false;

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

void extract_table_meta_data(const std::string folder_name) {
  // TODO: why not use the CSV exporter?
  auto table_to_csv = [](const std::string table_name, const std::string csv_file_name) {
    const auto table = SQLPipelineBuilder{"SELECT * FROM " + table_name}
                          .create_pipeline()
                          .get_result_table().second;
    std::ofstream output_file(csv_file_name);

    const auto column_names = table->column_names();
    for (auto column_id = size_t{0}; column_id < column_names.size(); ++column_id) {
      auto column_name = column_names[column_id];
      boost::to_upper(column_name);
      output_file << column_name;
      if (column_id < (column_names.size() - 1)) {
        output_file << "|";
      }
    }
    output_file << std::endl;

    const auto row_count = table->row_count();
    const auto data_types = table->column_data_types();
    for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
      const auto row = table->get_row(row_index);
      for (auto column_id = size_t{0}; column_id < row.size(); ++column_id) {
        if (data_types[column_id] == DataType::String) output_file << "\"";
        output_file << row[column_id];
        if (data_types[column_id] == DataType::String) output_file << "\"";
        if (column_id < (row.size() - 1)) {
        output_file << "|";
        }
      }
      output_file << std::endl;
    }
  };

  //table_to_csv("meta_segments", folder_name + "/segment_meta_data2.csv");
  table_to_csv("meta_segments_accurate", folder_name + "/segment_meta_data.csv");
  table_to_csv("meta_tables", folder_name + "/table_meta_data.csv");
  table_to_csv("meta_columns", folder_name + "/column_meta_data.csv");
  table_to_csv("meta_chunks", folder_name + "/chunk_meta_data.csv");
}

}  // namespace

std::string Driver::description() const { return "This driver executes benchmarks and outputs its plan cache to an array of CSV files."; }

void Driver::start() {
  const auto BENCHMARKS = std::vector<std::string>{"TPC-C", "TPC-DS", "JOB", "TPC-H", "CH"}; 
  const auto ENCODINGS = std::vector<std::string>{"DictionaryFSBA", "DictionarySIMDBP128", "Unencoded",
                                                  "LZ4", "RunLength", "FixedStringFSBAAndFrameOfReferenceFSBA",
                                                  "FixedStringSIMDBP128AndFrameOfReferenceSIMDBP128"}; 
  auto main_encoding = ENCODINGS[0];

  constexpr auto RELEASE = true;

  const auto env_var_benchmark = std::getenv("BENCHMARK_TO_RUN");
  if (env_var_benchmark == NULL) {
    std::cerr << "Please pass environment variable \"BENCHMARK_TO_RUN\" to set a target benchmark.\nExiting Plugin." << std::flush;
    exit(17);
  }

  const auto env_var_encoding = std::getenv("ENCODING_TO_USE");
  const auto env_var_encoding_config = std::getenv("ENCODING_CONFIG");
  if (env_var_encoding == NULL && env_var_encoding_config == NULL) {
    std::cout << "Encoding data with the default of " << main_encoding << std::endl;
  } else if (env_var_encoding != NULL) {
    main_encoding = std::string{env_var_encoding};
    std::cout << "Encoding data with " << main_encoding << std::endl;
  } 

  auto BENCHMARK = std::string(env_var_benchmark);
  if (std::find(BENCHMARKS.begin(), BENCHMARKS.end(), BENCHMARK) == BENCHMARKS.end()) {
    std::cerr << "Benchmark \"" << BENCHMARK << "\" not supported. Supported benchmarks: ";
    for (const auto& benchmark : BENCHMARKS) std::cout << "\"" << benchmark << "\" ";
    std::cerr << "\nExiting." << std::flush;
    exit(17);
  }
  if (std::find(ENCODINGS.begin(), ENCODINGS.end(), main_encoding) == ENCODINGS.end()) {
    std::cerr << "Encoding \"" << main_encoding << "\" not supported. Supported encodings: ";
    for (const auto& encoding : ENCODINGS) std::cout << "\"" << encoding << "\" ";
    std::cerr << "\nExiting." << std::flush;
    exit(17);
  }
  std::cout << "Running " << BENCHMARK << " ... " << std::endl;

  auto encoding_config = EncodingConfig{};
  if (main_encoding == "DictionaryFSBA") {
    encoding_config = EncodingConfig(SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned});
  } else if (main_encoding == "DictionarySIMDBP128") {
    encoding_config = EncodingConfig(SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128});
  } else if (main_encoding == "Unencoded") {
    encoding_config = EncodingConfig(SegmentEncodingSpec{EncodingType::Unencoded});
  } else if (main_encoding == "LZ4") {
    encoding_config = EncodingConfig(SegmentEncodingSpec{EncodingType::LZ4});
  } else if (main_encoding == "RunLength") {
    encoding_config = EncodingConfig(SegmentEncodingSpec{EncodingType::RunLength});
  } else if (main_encoding == "FixedStringFSBAAndFrameOfReferenceFSBA") {
    // Passing a default of dictionary encoding, use FoR for integers and FString for strings.
    std::unordered_map<DataType, SegmentEncodingSpec> mapping = {
        {DataType::Int, SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::FixedSizeByteAligned}},
        {DataType::String, SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::FixedSizeByteAligned}}
      };
    encoding_config = EncodingConfig(SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
      mapping, std::unordered_map<std::string, std::unordered_map<std::string, SegmentEncodingSpec>>{});
  } else if (main_encoding == "FixedStringSIMDBP128AndFrameOfReferenceSIMDBP128") {
    // Passing a default of dictionary encoding, use FoR for integers and FString for strings.
    std::unordered_map<DataType, SegmentEncodingSpec> mapping = {
        {DataType::Int, SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128}},
        {DataType::String, SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::SimdBp128}}
      };
    encoding_config = EncodingConfig(SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
      mapping, std::unordered_map<std::string, std::unordered_map<std::string, SegmentEncodingSpec>>{});
  }

  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  config->encoding_config = encoding_config;
  if (env_var_encoding_config != NULL) {
    std::cout << "Using a custom encoding config: " << std::string{env_var_encoding_config} << std::endl;
    auto folder_name = std::string{env_var_encoding_config};
    folder_name = folder_name.substr(folder_name.rfind("/") + 1);
    folder_name = folder_name.substr(0, folder_name.size() - 5);
    main_encoding = folder_name;
    config->encoding_config = EncodingConfig(CLIConfigParser::parse_encoding_config(std::string{env_var_encoding_config}));
  }
  config->max_runs = 10;
  config->enable_visualization = false;
  //config->cache_binary_tables = BENCHMARK != "TPC-C" ? true : false;
  config->cache_binary_tables = false;  // There might still be problems with binaries files, safe but slow route
  config->max_duration = std::chrono::seconds(300);
  config->warmup_duration = std::chrono::seconds(0);
  //config->cache_binary_tables = false; // might be necessary due to some problems with binary exports :(

  constexpr auto USE_PREPARED_STATEMENTS = false;
  auto SCALE_FACTOR = 17.0f;  // later overwritten


  // Set caches (DOES NOT WORK ... set in abstract cache for now. Benchmarks reset the cache.)
  // Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>(100'000);
  // Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>(100'000);

  //
  //  TPC-H
  //
  if (BENCHMARK == "TPC-H") {
    SCALE_FACTOR = RELEASE ? 10.0f : 1.0f;
    config->max_runs = RELEASE ? 100 : 1;

    //const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{2}};
    //auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR, tpch_query_ids_benchmark);
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
    SCALE_FACTOR = RELEASE ? 5.0f : 1.0f;
    config->max_runs = 100;
    const std::string query_path = "hyrise/resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";
    if (!std::filesystem::exists("resources/")) {
      std::cout << "When resources for TPC-DS cannot be found, create a symlink as a workaround: 'ln -s hyrise/resources resources'." << std::endl;
    }

    //auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, filename_blacklist(), std::unordered_set<std::string>{"1"});
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
    config->max_runs = 10;

    const auto table_path = "hyrise/imdb_data";
    const auto query_path = "hyrise/third_party/join-order-benchmark";
    const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

    //auto benchmark_item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, non_query_file_names, std::unordered_set<std::string>{"10a"});
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

  //
  //  TPC-C
  //
  else if (BENCHMARK == "TPC-C" or BENCHMARK == "CH") {
    auto warehouse_count = int{1};
    config->max_duration = std::chrono::seconds{20};

    if (BENCHMARK == "CH") {
      warehouse_count = 10;
      config->max_duration = std::chrono::seconds{300};
    }

    config->max_runs = -1;
    config->benchmark_mode = BenchmarkMode::Shuffled;
    config->warmup_duration = std::chrono::seconds(0);
    config->enable_scheduler = true;
    config->clients = 5;
    config->cores = 10;

    if (USE_MVCC_DELETE) {
      // We do not use the MVCC delete plugin for now, as we write by far to few lines to actually make a difference.
      auto& pm = Hyrise::get().plugin_manager;
      pm.load_plugin("./rel/lib/libMvccDeletePlugin.dylib");
    }

    auto context = BenchmarkRunner::create_context(*config);
    context.emplace("scale_factor", warehouse_count);

    auto run_ch_benchmark_queries = std::atomic<bool>{false};
    auto ch_benchmark_queries = std::vector<std::string>{};
    if (BENCHMARK == "CH") {
      constexpr auto TPC_H_SCALE_FACTOR = 1.0f;
      auto tpch_table_generator = std::make_unique<TPCHTableGenerator>(TPC_H_SCALE_FACTOR, config);
      tpch_table_generator->generate_and_store();

      const auto ch_benchmark_queries_path = "hyrise/resources/ch_benchmark_queries.sql";
      std::ifstream ch_benchmark_queries_file(ch_benchmark_queries_path);

      std::string sql_query_string;
      while (std::getline(ch_benchmark_queries_file, sql_query_string)) {
        if (sql_query_string.size() > 0 && !sql_query_string.starts_with("--")) {
          ch_benchmark_queries.emplace_back(sql_query_string);
        }
      }
      Assert(ch_benchmark_queries.size() > 0, "Failed to read CH-benCHmark queries.");
    }

    auto ch_benchmark_thread = std::thread([&ch_benchmark_queries, &run_ch_benchmark_queries]() {
      while (!run_ch_benchmark_queries) {
        std::this_thread::sleep_for(1s);
      }

      auto& storage_manager = Hyrise::get().storage_manager;
      while (run_ch_benchmark_queries && !storage_manager.has_table("ITEM")) {
        std::this_thread::sleep_for(1s);
      }

      std::cout << "Starting CH-benCHmark queries in 1s." << std::endl;
      std::this_thread::sleep_for(1s);

      // auto sql_pipeline2 = SQLPipelineBuilder{"SELECT SUM(S_ORDER_CNT) * .005 FROM STOCK, supplier, nation WHERE S_W_ID * S_I_ID = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'"}.create_pipeline();
      // const auto [status, result] = sql_pipeline2.get_result_table();
      // Print::print(result);

      auto query_id = size_t{0};
      const auto query_count = ch_benchmark_queries.size();
      while (run_ch_benchmark_queries) {
        query_id = query_id % query_count;
        std::cout << "CH-benCHmark - Query #" << query_id << ": ";
        auto sql_pipeline = SQLPipelineBuilder{ch_benchmark_queries[query_id]}.create_pipeline();
        Timer timer;
        const auto& [status, result] = sql_pipeline.get_result_table();
        std::cout << static_cast<size_t>(timer.lap().count()) << " ns";
        if (result->row_count() < 1) {
          std::cout << " (empty result: " << ch_benchmark_queries[query_id].substr(0, 100) << "...)";
        }
        std::cout << std::endl;

        Assert(status == SQLPipelineStatus::Success, "Execution of query #" + std::to_string(query_id) + " did not succeed.");
        ++query_id;
      }
      std::cout << "Analytical CH-benCHmark queries finished." << std::endl;
    });

    TPCCTableGenerator(warehouse_count, config).generate_and_store();
    if (BENCHMARK == "CH") {
      run_ch_benchmark_queries = true;
    }

    auto item_runner = std::make_unique<TPCCBenchmarkItemRunner>(config, warehouse_count);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(item_runner), nullptr, context);

    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
    std::cout << "TPC-C done." << std::endl;

    if (BENCHMARK == "CH") {
      run_ch_benchmark_queries = false;
    }

  std::string folder_name = std::string(BENCHMARK) + "__SF_" + std::to_string(SCALE_FACTOR);
  folder_name += "__RUNS_" + std::to_string(config->max_runs) + "__ENCODING_" + main_encoding;
  std::filesystem::create_directories(folder_name);

  std::cout << "Exporting table/column/segments meta data." << std::endl;
  extract_table_meta_data(folder_name);

  if (Hyrise::get().default_pqp_cache->size() > 0) {
    std::cout << "Exporting plan cache data." << std::endl;
    PlanCacheCsvExporter(folder_name).run();
  } else {
    std::cerr << "Plan cache is empty." << std::endl;
    exit(17);
  }

    if (ch_benchmark_thread.joinable()) {
      ch_benchmark_thread.join();
    }

    if (USE_MVCC_DELETE) {
      std::cout << "Number of log entries: " << Hyrise::get().log_manager.log_entries().size() << std::endl;
      for (const auto& entry : Hyrise::get().log_manager.log_entries()) {
        const auto timestamp_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(entry.timestamp.time_since_epoch()).count();

        // We need this to format the timestamp in a thread-safe way.
        // https://stackoverflow.com/questions/25618702/
        //   why-is-there-no-c11-threadsafe-alternative-to-stdlocaltime-and-stdgmtime
        std::ostringstream timestamp;
        auto time = std::chrono::system_clock::to_time_t(entry.timestamp);
        struct tm buffer {};
        timestamp << std::put_time(localtime_r(&time, &buffer), "%F %T");
        std::cout << timestamp_ns << ":" << pmr_string{timestamp.str()} << ":" << pmr_string{log_level_to_string.left.at(entry.log_level)} << ":" << static_cast<int32_t>(entry.log_level) << ":" << pmr_string{entry.reporter} << ":" << pmr_string{entry.message} << std::endl;
      }
    }
  }
  //
  //  /TPC-C
  //

  std::string folder_name = std::string(BENCHMARK) + "__SF_" + std::to_string(SCALE_FACTOR);
  folder_name += "__RUNS_" + std::to_string(config->max_runs) + "__ENCODING_" + main_encoding;
  std::filesystem::create_directories(folder_name);

  std::cout << "Exporting table/column/segments meta data." << std::endl;
  extract_table_meta_data(folder_name);

  if (Hyrise::get().default_pqp_cache->size() > 0) {
    std::cout << "Exporting plan cache data." << std::endl;
    PlanCacheCsvExporter(folder_name).run();
  } else {
    std::cerr << "Plan cache is empty." << std::endl;
    exit(17);
  }

  std::cout << "Done." << std::endl;

  if (BENCHMARK == "TPC-C" && USE_MVCC_DELETE) {
    // Not unloading as it seems to hang forever.
    Hyrise::get().plugin_manager.unload_plugin("MvccDeletePlugin");
  }
}

void Driver::stop() {
}

EXPORT_PLUGIN(Driver)
