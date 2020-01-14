#include "sql/sql_pipeline_builder.hpp"

#include "driver.hpp"

namespace opossum {

const std::string Driver::description() const { return "This driver executes benchmarks and outputs its parsed plan cache to an array of CSV files."; }

void Driver::start() {
  if (SQLPipelineBuilder::default_pqp_cache->size() > 0) {
  	std::cout << "Exporting plan cache ... ";
    PlanCacheCsvExporter();
    std::cout << "done." << std::endl;
  } else {
    std::cout << "Plan cache is empty." << std::endl;
  }
}

void Driver::stop() {
}

EXPORT_PLUGIN(Driver)

}  // namespace opossum
