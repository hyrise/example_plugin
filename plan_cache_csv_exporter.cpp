#include <thread>

#include "sql/sql_plan_cache.hpp"
#include "server/server.hpp"

#include "driver.hpp"

namespace opossum {

constexpr int64_t START_BUDGET = 74'000'000;

const std::string Driver::description() const { return "This is the Hyrise Driver"; }

void Driver::_init() {
  _run = true;
  _settings["Budget"] = AllTypeVariant{START_BUDGET};
}

void Driver::start() {
  
  // const auto scale_factor = 0.01f;

  // std::shared_ptr<BenchmarkConfig> config;
  // auto context = BenchmarkRunner::create_context(*config);

  // Run the benchmark
  // auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, scale_factor, item_ids);
  // BenchmarkRunner(*config, std::move(item_runner), std::make_unique<TpchTableGenerator>(scale_factor, config), context)
  //     .run();

  if (SQLPhysicalPlanCache::get().size() >= 1) {
    const auto forecasted_workload = _wp.get_forecasts();
    // StorageManager::get().get_server()->toggle_dont_accept_next_connection();
    // std::this_thread::sleep_for(std::chrono::milliseconds(100));

    
    // _it.create_indexes_for_workload(forecasted_workload, static_cast<size_t>(boost::get<int64_t>(_settings["Budget"])));
    // SQLPhysicalPlanCache::get().clear();
    // SQLLogicalPlanCache::get().clear();

    // StorageManager::get().get_server()->toggle_dont_accept_next_connection();
  }
}

void Driver::stop() {
  _run = false;
  _main_thread->join();

  delete &Driver::get();
}

void Driver::update_setting(const std::string& key, const AllTypeVariant& value) {
  std::cout << key << " was set to: " << value << std::endl;
  _settings[key] = value;
}

EXPORT_PLUGIN(Driver)

}  // namespace opossum
