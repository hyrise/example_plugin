#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

#include "plan_cache_csv_exporter.hpp"

namespace opossum {

class Driver : public AbstractPlugin, public Singleton<Driver> {
 public:
  const std::string description() const final;

  void start() final;

  void stop() final;
};

}  // namespace opossum
