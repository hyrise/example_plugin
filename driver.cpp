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
  _init();

  _main_thread = std::make_shared<std::thread>(([=]() {
    while (_run) {
      std::cout << "Driver running" << std::endl;
      // auto br = StorageManager::get().get_benchmark_runner();
      // if (SQLPhysicalPlanCache::get().size() >= 1  && !br->runs()) {
      if (SQLPhysicalPlanCache::get().size() >= 1) {
        std::cout << "Driver Optimizing" << std::endl;
        // if (StorageManager::get())
        if (StorageManager::get().get_server() == nullptr) {
          std::cout << "NULLPTR" << std::endl;
        } else {
          StorageManager::get().get_server()->toggle_dont_accept_next_connection();
          std::this_thread::sleep_for(std::chrono::milliseconds(100));

          const auto forecasted_workload = _wp.get_forecasts();
          _it.create_indexes_for_workload(forecasted_workload, static_cast<size_t>(boost::get<int64_t>(_settings["Budget"])));
          SQLPhysicalPlanCache::get().clear();
          SQLLogicalPlanCache::get().clear();

          StorageManager::get().get_server()->toggle_dont_accept_next_connection();
        }
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(5'000));
    }
  }));
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
