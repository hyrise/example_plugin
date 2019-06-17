#pragma once

#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

#include "index_tuner_advanced.hpp"
#include "workload_predictor.hpp"

namespace opossum {

class DriverAdvanced : public AbstractPlugin, public Singleton<DriverAdvanced> {
 public:
  const std::string description() const final;

  void start() final;

  void stop() final;

  void update_setting(const std::string& key, const AllTypeVariant& value);
 private:
  IndexTunerAdvanced _it;
  WorkloadPredictor _wp;
  bool _run = true;
  std::shared_ptr<std::thread> _main_thread;

  void _init();
};

}  // namespace opossum
