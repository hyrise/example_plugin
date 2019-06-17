#include <thread>

#include "sql/sql_plan_cache.hpp"
#include "server/server.hpp"

#include "driver.hpp"

namespace opossum {

const std::string Driver::description() const { return "This is the Hyrise Driver"; }

void Driver::start() {
  _run = true;

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
          _it.create_indexes_for_workload(forecasted_workload, BUDGET);
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

EXPORT_PLUGIN(Driver)

}  // namespace opossum
