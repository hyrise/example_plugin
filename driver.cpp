#include <thread>

#include "sql/sql_plan_cache.hpp"

#include "driver.hpp"

namespace opossum {

const std::string Driver::description() const { return "This is the Hyrise Driver"; }

void Driver::start() {
  std::thread([=]() {
    while (true) {
      auto br = StorageManager::get().get_benchmark_runner();
      if (SQLPhysicalPlanCache::get().size() >= 1  && !br->runs()) {
        const auto forecasted_workload = _wp.get_forecasts();
        _it.create_indexes_for_workload(forecasted_workload, BUDGET);
        SQLPhysicalPlanCache::get().clear();
        SQLLogicalPlanCache::get().clear();
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(1'000));
    }}).detach();
}

void Driver::stop() { }

EXPORT_PLUGIN(Driver)

}  // namespace opossum
