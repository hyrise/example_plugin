#include "driver.hpp"

namespace opossum {

const std::string Driver::description() const { return "This is the Hyrise Driver"; }

void Driver::start() {
  const auto forecasted_workload = _wp.get_forecasts();
}

void Driver::stop() { }

EXPORT_PLUGIN(Driver)

}  // namespace opossum