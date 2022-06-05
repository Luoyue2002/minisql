#include "catalog/table.h"

/*
  store the tablemetadata in the format: 
  | TABLE_MD_MAGIC_NUM int | table_id_ int 
  | table_name_(len int + name char*)
  | root_page_id_ int | schema_
*/


uint32_t TableMetadata::SerializeTo(char *buf) const {
  
  //MAGIC_NUM
  MACH_WRITE_TO(uint32_t, buf, TABLE_METADATA_MAGIC_NUM); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  
  // table_id_
  MACH_WRITE_TO(uint32_t, buf, table_id_); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  
  //table_name_len + table_name
  MACH_WRITE_TO(uint32_t, buf, table_name_.size()); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  std::memmove(buf, table_name_.data(), table_name_.size());
  buf += table_name_.size()*sizeof(char);

  //root_page_id_
  MACH_WRITE_TO(uint32_t, buf, root_page_id_); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  //schema serialize in schema.cpp  
  buf += schema_->SerializeTo(buf);
  
  return GetSerializedSize();
}

uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t size=0;
  size += sizeof(uint32_t); // MAGIC_NUM
  size += sizeof(uint32_t); // table_id_
  size += sizeof(uint32_t); // index_name len
  size += table_name_.size() * sizeof(char); // name char*
  size += sizeof(uint32_t); // root_page_id
  return size;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  if (table_meta != nullptr) return 0;
  
  uint32_t MAGIC_NUM;
  MAGIC_NUM = MACH_READ_FROM(u_int32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  if(MAGIC_NUM != TABLE_METADATA_MAGIC_NUM) return 0;
  
  table_id_t table_id_ = 0;
  uint32_t name_len=0;
  std::string table_name_="";
  page_id_t root_page_id_ = 0;

  // table_id_
  table_id_ = MACH_READ_FROM(u_int32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  //table_name_len + char*
  name_len = MACH_READ_FROM(u_int32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  table_name_.assign(buf, name_len);
  buf += name_len *sizeof(char);

  //root_page_id_
  root_page_id_ = MACH_READ_FROM(u_int32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  //Schema
  Schema *schema_ = NULL;
  uint32_t cnt = Schema::DeserializeFrom(buf, schema_, heap);
  buf += cnt;
  table_meta = reinterpret_cast<TableMetadata*>(heap->Allocate(sizeof(TableMetadata)));
  if(table_meta == nullptr){
    // alloc failed
    return 0;
  }
  new(table_meta) TableMetadata(table_id_, table_name_, root_page_id_, schema_);

  return table_meta->GetSerializedSize();
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}

