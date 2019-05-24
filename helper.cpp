#include "helper.hpp"

size_t predict_index_size(const std::string& table_name, opossum::ColumnID column_id) {
  const auto table = opossum::StorageManager::get().get_table(table_name);
  size_t index_size = 0;

  for (const auto& chunk : table->chunks()) {
    const auto segment = chunk->get_segment(column_id);
    const auto dict_segment = std::dynamic_pointer_cast<const opossum::BaseDictionarySegment>(segment);
    const auto unique_values = dict_segment->unique_values_count();
    index_size +=
        opossum::BaseIndex::estimate_memory_consumption(opossum::SegmentIndexType::GroupKey, chunk->size(), unique_values, /*Last Parameter is ignored for GroupKeyIndex*/1);
  }

  return index_size;
}
