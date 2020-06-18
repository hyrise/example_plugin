#pragma once

#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class Driver : public AbstractPlugin, public Singleton<Driver> {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;
};

}  // namespace opossum
