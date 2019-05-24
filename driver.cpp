#include "driver.hpp"

namespace opossum {

const std::string Driver::description() const { return "This is the Hyrise Driver"; }

void Driver::start() {
  const auto forecasted_workload = _wp.get_forecasts();
  _it.create_indexes_for_workload(forecasted_workload, BUDGET);
}

void Driver::stop() { }

EXPORT_PLUGIN(Driver)

}  // namespace opossum
