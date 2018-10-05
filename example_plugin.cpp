#include "example_plugin.hpp"

#include "storage/table.hpp"

namespace opossum {

const std::string ExamplePlugin::description() const { return "This is the Hyrise ExamplePlugin"; }

void ExamplePlugin::start() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_1", DataType::Int);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  sm.add_table("DummyTable", table);
}

void ExamplePlugin::stop() { StorageManager::get().drop_table("DummyTable"); }

EXPORT_PLUGIN(ExamplePlugin)

}  // namespace opossum
