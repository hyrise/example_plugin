#pragma once

#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class Driver : public AbstractPlugin, public Singleton<Driver> {
 public:
  Driver() : sm(StorageManager::get()) {}

  const std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& sm;
};

}  // namespace opossum
