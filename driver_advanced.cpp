#include <thread>

#include "sql/sql_plan_cache.hpp"
#include "server/server.hpp"

#include "driver_advanced.hpp"

namespace opossum {

constexpr int64_t START_BUDGET = 74'000'000;

const std::string DriverAdvanced::description() const { return "This is the Hyrise DriverAdvanced"; }

void DriverAdvanced::_init() {
  _run = true;
  _settings["Budget"] = AllTypeVariant{START_BUDGET};
}

void DriverAdvanced::start() {
  _init();

  _main_thread = std::make_shared<std::thread>(([=]() {
    while (_run) {
      std::cout << "DriverAdvanced running" << std::endl;
      if (SQLPhysicalPlanCache::get().size() >= 1) {
        std::cout << "DriverAdvanced Optimizing" << std::endl;
        StorageManager::get().get_server()->toggle_dont_accept_next_connection();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        const auto forecasted_workload = _wp.get_forecasts();
        _it.create_indexes_for_workload(forecasted_workload, static_cast<size_t>(boost::get<int64_t>(_settings["Budget"])));
        SQLPhysicalPlanCache::get().clear();
        SQLLogicalPlanCache::get().clear();

        StorageManager::get().get_server()->toggle_dont_accept_next_connection();
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(5'000));
    }
  }));
}

void DriverAdvanced::stop() {
  _run = false;
  _main_thread->join();

  delete &DriverAdvanced::get();
}

void DriverAdvanced::update_setting(const std::string& key, const AllTypeVariant& value) {
  std::cout << key << " was set to: " << value << std::endl;
  _settings[key] = value;
}

EXPORT_PLUGIN(DriverAdvanced)

}  // namespace opossum
