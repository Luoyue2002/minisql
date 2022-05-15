#include "record/schema.h"

/*
  The Schema is serialized in this format: 
    | SCHEMA_MAGIC_NUM | len unsigned int | column1 | ... | column n |
*/

uint32_t Schema::SerializeTo(char *buf) const {
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  MACH_WRITE_TO(uint32_t, buf, columns_.size());
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  for(u_int32_t i = 0; i < columns_.size(); i++) {
    buf += columns_[i]->SerializeTo(buf);
  }
  return GetSerializedSize();
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = Type::GetTypeSize(TypeId::kTypeInt)*2;
  for(u_int32_t i = 0; i < columns_.size(); i++)
    size += columns_[i]->GetSerializedSize(); 
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  if (schema != nullptr) {
    return 0;
  }

  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  if (MAGIC_NUM != 200715) return 0;
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  uint32_t len = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  std::vector<Column *> columns;
  Column* temp;
  for(u_int32_t i = 0; i < len; i++) {
    buf += Column::DeserializeFrom(buf, temp, heap);
    columns.push_back(temp);
  }

  schema = ALLOC_P(heap, Schema)(columns);
  return schema->GetSerializedSize();
}