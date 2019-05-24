#pragma once

#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

#include "index_tuner.hpp"
#include "workload_predictor.hpp"

namespace opossum {

class Driver : public AbstractPlugin, public Singleton<Driver> {
 public:
  const std::string description() const final;

  void start() final;

  void stop() final;
 private:
  IndexTuner _it;
  WorkloadPredictor _wp;
};

}  // namespace opossum
