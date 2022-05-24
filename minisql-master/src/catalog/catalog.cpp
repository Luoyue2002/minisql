#include "catalog/catalog.h"

/*
  | MAGIC_NUM | 
  | table_meta_pages_: n(size) + n*(table_id, page_id)
  | index_meta_pages_: n(size) + n*(index_id, page_id)

 */
void CatalogMeta::SerializeTo(char *buf) const {
  //MAGIC_NUM  
  MACH_WRITE_TO(uint32_t, buf, CATALOG_METADATA_MAGIC_NUM); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  // table_meta_pages_
  MACH_WRITE_TO(uint32_t, buf, table_meta_pages_.size()); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  auto it = table_meta_pages_.begin();
  for(;it!=table_meta_pages_.end();it++){
    MACH_WRITE_TO(uint32_t, buf, it->first); 
    buf += Type::GetTypeSize(TypeId::kTypeInt);
    MACH_WRITE_TO(uint32_t, buf, it->second); 
    buf += Type::GetTypeSize(TypeId::kTypeInt);
  }
  // index_meta_pages_
  MACH_WRITE_TO(uint32_t, buf, index_meta_pages_.size());
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  for(it = index_meta_pages_.begin(); it != index_meta_pages_.end(); it++){
    MACH_WRITE_TO(uint32_t, buf, it->first); 
    buf += Type::GetTypeSize(TypeId::kTypeInt);
    MACH_WRITE_TO(uint32_t, buf, it->second); 
    buf += Type::GetTypeSize(TypeId::kTypeInt);
  }

}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  uint32_t MAGIC_NUM;
  MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  if(MAGIC_NUM != CATALOG_METADATA_MAGIC_NUM) return 0;

  uint32_t table_id_=0;
  uint32_t index_id_=0;
  uint32_t page_id_ =0;
  uint32_t Size = 0;
  std::map<table_id_t, page_id_t> table_meta_pages_;
  std::map<index_id_t, page_id_t> index_meta_pages_;
  //index_meta_page
  Size = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  for(uint32_t i=0; i<Size; ++i){
    table_id_ = MACH_READ_FROM(uint32_t, buf);
    buf += Type::GetTypeSize(TypeId::kTypeInt);
    page_id_ = MACH_READ_FROM(int32_t, buf);
    buf += Type::GetTypeSize(TypeId::kTypeInt);
    table_meta_pages_[table_id_] = page_id_;
  }
  // index_meta_page
  Size = MACH_READ_FROM(uint32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  for(uint32_t i=0; i<Size; ++i){
    index_id_ = MACH_READ_FROM(uint32_t, buf);
    buf += Type::GetTypeSize(TypeId::kTypeInt);
    page_id_ = MACH_READ_FROM(int32_t, buf);
    buf += Type::GetTypeSize(TypeId::kTypeInt);
    index_meta_pages_[index_id_] = page_id_;
  }
  CatalogMeta *CatalogMeta_ = nullptr;
  CatalogMeta_ = reinterpret_cast<CatalogMeta*>(heap->Allocate(sizeof(CatalogMeta)));
  if(CatalogMeta_ == nullptr) return 0;

  new(CatalogMeta_) CatalogMeta(table_meta_pages_,index_meta_pages_);

  return CatalogMeta_;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof(uint32_t); // MAGIC_NUM
  size += sizeof(uint32_t); // table_meta_pages_ size  
  size += sizeof(uint32_t); // index_meta_pages_ size
  size += sizeof(uint32_t) * table_meta_pages_.size() + sizeof(int32_t) * table_meta_pages_.size();
  size += sizeof(uint32_t) * index_meta_pages_.size() + sizeof(int32_t) * index_meta_pages_.size();
  return size;
}

CatalogMeta::CatalogMeta() {}
CatalogMeta::CatalogMeta(std::map<table_id_t, page_id_t>tablepage_,std::map<index_id_t, page_id_t>indexpage_)
    :table_meta_pages_(tablepage_),index_meta_pages_(indexpage_) {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}
