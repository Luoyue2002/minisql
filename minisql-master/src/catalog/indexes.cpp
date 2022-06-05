#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

/*
  | MAGIC_NUM | index_id_ | table_id_ | index_name_ (len+data)|
  | key_map (size+data)|



 */


uint32_t IndexMetadata::SerializeTo(char *buf) const {
  //MAGIC_NUM
  MACH_WRITE_TO(uint32_t, buf, INDEX_METADATA_MAGIC_NUM); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  //index_id_
  MACH_WRITE_TO(uint32_t, buf, index_id_); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  //table_id_
  MACH_WRITE_TO(uint32_t, buf, table_id_); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  //index_name_ size + data
  MACH_WRITE_TO(uint32_t, buf, index_name_.size()); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);  
  std::memmove(buf, index_name_.data(), index_name_.size());
  buf += index_name_.size() * sizeof(char);

  //key_map_ size + data
  MACH_WRITE_TO(uint32_t, buf, key_map_.size()); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  std::memmove(buf, key_map_.data(), key_map_.size()*sizeof(uint32_t));
  buf += key_map_.size()*sizeof(uint32_t);

  return GetSerializedSize();
}

uint32_t IndexMetadata::GetSerializedSize() const {
  uint32_t size=0;
  size += Type::GetTypeSize(TypeId::kTypeInt);  //MAGIC_NUM
  size += Type::GetTypeSize(TypeId::kTypeInt);  //index_id_
  size += Type::GetTypeSize(TypeId::kTypeInt);  //table_id_
  size += Type::GetTypeSize(TypeId::kTypeInt);   //index_name_ size
  size += index_name_.size() * sizeof(char);  //index_name_ data
  size += Type::GetTypeSize(TypeId::kTypeInt);  //key_map_ size
  size += key_map_.size() * sizeof(uint32_t); //key_map_ data
  return size; 
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  if(index_meta != 0) return 0; // is it neccesary?
  
  uint32_t MAGIC_NUM;
  MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  if (MAGIC_NUM != INDEX_METADATA_MAGIC_NUM) return 0;
  
  index_id_t index_id_ = 0;
  table_id_t table_id_ = 0;
  std::string index_name_ = "";
  uint32_t name_len = 0;

  std::vector<uint32_t> key_map_;
  uint32_t key_map_size;
  uint32_t key_;
  

  //index_id_
  index_id_ = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  //table_id_
  table_id_ = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  //index_name_ size+data
  name_len = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  index_name_.assign(buf, name_len);
  buf += name_len * sizeof(char);
  //key_map
  key_map_size = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  for (uint32_t i=0; i<key_map_size; ++i){
    key_ = MACH_READ_FROM(uint32_t, buf);
    buf += Type::GetTypeSize(TypeId::kTypeInt);
    key_map_.push_back(key_);
  }
  
  //
  index_meta = reinterpret_cast<IndexMetadata *>(heap->Allocate(sizeof(IndexMetadata)));
  if(index_meta == nullptr){
    //alloc failed 
    return 0;
  }
  new (index_meta) IndexMetadata(index_id_, index_name_, table_id_, key_map_);
  
  return index_meta->GetSerializedSize();
}
