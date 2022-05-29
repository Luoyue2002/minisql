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
  // LOAD DATA
  if(init){
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    next_table_id_=0;
    next_index_id_=0;
  }else{
    Page *META_PAGE = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(META_PAGE->GetData(), heap_);
    std::map<table_id_t, page_id_t> *gotTablePages = catalog_meta_->GetTableMetaPages();
    for(auto it = gotTablePages->begin(); it != gotTablePages->end(); it++){
    // printf("WTTTTTFFF2\n");
      if(it->second != INVALID_PAGE_ID)
        LoadTable(it->first, it->second);
    }
    std::map<index_id_t, page_id_t> *gotIndexPages = catalog_meta_->GetIndexMetaPages();
    for(auto it = gotIndexPages->begin(); it != gotIndexPages->end(); it++){
      if(it->second != INVALID_PAGE_ID)
        LoadIndex(it->first, it->second);
    }
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
  }

}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {

  /*
    1. if table existed
    2. if table_attr conflict --not implemented yet
    3. create catalog
    4. write in
   */

  if(table_names_.find(table_name)!=table_names_.end()){
    return DB_TABLE_ALREADY_EXIST;
  }
  table_id_t now_table_id = next_table_id_;
  TableHeap *tableheap_;
  TableMetadata *table_meta;
  page_id_t new_page_id;
  Page * new_page = buffer_pool_manager_->NewPage(new_page_id);
  if(new_page==nullptr) return DB_FAILED;
  
  {// catalog_meta update

  (*(catalog_meta_->GetTableMetaPages()))[now_table_id] = new_page_id;
  (*(catalog_meta_->GetTableMetaPages()))[now_table_id+1] = INVALID_PAGE_ID;
  Page * catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  
  }
  { // table_info create
  table_info = TableInfo::Create(heap_);
  tableheap_ = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, table_info->GetMemHeap());
  table_meta = TableMetadata::Create(now_table_id, table_name, tableheap_->GetFirstPageId(), schema, table_info->GetMemHeap());
  if(table_info == nullptr || table_meta == nullptr || tableheap_ == nullptr){
    return DB_FAILED;
  }
  table_meta->SerializeTo(new_page->GetData());
  table_info->Init(table_meta, tableheap_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  }
  {// catalog mgr update
  next_table_id_++;  
  table_names_.emplace(table_name, now_table_id);
  tables_.emplace(now_table_id, table_info);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto got = table_names_.find(table_name);
  if(got == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_.find(got->second)->second;
  if(!table_info)return DB_TABLE_NOT_EXIST;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");

  if(tables_.empty()){
    return DB_TABLE_NOT_EXIST;
  }
  std::unordered_map<table_id_t, TableInfo *>::const_iterator got;


  for(got = tables_.begin(); got != tables_.end(); got++){
    tables.push_back(got->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  /*
    1. table existed?
    2. index already existed?
    3. column illegal
    4. alloc success?
    5. write in
   */
  if(table_names_.find(table_name)==table_names_.end()){
    return DB_TABLE_NOT_EXIST; 
  }
  
  auto gotindexes = index_names_.find(table_name);
  if(gotindexes != index_names_.end()){

    if((gotindexes->second).find(index_name)!=(gotindexes->second).end()){
      return DB_INDEX_ALREADY_EXIST;
    }
  }

  index_id_t now_index_id = next_index_id_;
  table_id_t cur_table_id;
  page_id_t new_page_id;
  Page *new_page;

  cur_table_id = table_names_.find(table_name)->second;
  TableInfo *cur_table = tables_.find(cur_table_id)->second;
  TableSchema *table_schema = cur_table->GetSchema();
  IndexMetadata *index_meta;
  new_page = buffer_pool_manager_->NewPage(new_page_id);
  if(new_page == nullptr){
    // Page alloc failed
    return DB_FAILED;
  }
  { //catalog_meta update
    (*(catalog_meta_->GetIndexMetaPages()))[now_index_id] = new_page_id;
    (*(catalog_meta_->GetIndexMetaPages()))[now_index_id+1] = INVALID_PAGE_ID;
    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  }

  {

  index_info = IndexInfo::Create(heap_);
  if(index_info ==nullptr){
    return DB_FAILED;
  }
  std::vector<uint32_t> key_map;
  for (uint32_t i = 0; i < index_keys.size(); ++i) {
    uint32_t keys_idx=0;
    if(table_schema->GetColumnIndex(index_keys[i], keys_idx)){
      // cannot find the index name
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(keys_idx);
  }
  
  index_meta =  IndexMetadata::Create(now_index_id, index_name, cur_table_id, key_map, index_info->GetMemHeap());
  if(index_meta == nullptr){
    return DB_FAILED;
  }
  index_meta->SerializeTo(new_page->GetData());
  index_info->Init(index_meta, cur_table, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  }
  
  next_index_id_++;
  index_names_[table_name].emplace(index_name, now_index_id);
  indexes_.emplace(now_index_id, index_info);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
 
  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::const_iterator gotmp;
  gotmp = index_names_.find(table_name);
  if(gotmp == index_names_.end()) return DB_INDEX_NOT_FOUND;

  std::unordered_map<std::string, index_id_t>::const_iterator got;

  got = (gotmp->second).find(index_name);
  if(got == (gotmp->second).end())return DB_INDEX_NOT_FOUND;
  table_id_t table_id_=got->second;
  index_info = (indexes_.find(table_id_))->second; 

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {

  indexes.clear();
  if(index_names_.find(table_name)==index_names_.end()){
    return DB_INDEX_NOT_FOUND;
  }
  std::unordered_map<std::string, index_id_t> got = index_names_.find(table_name)->second;
  for(auto it=got.begin();it!=got.end();it++){
    indexes.push_back(indexes_.find(it->second)->second);
  }

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name)==table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }

  { // dropindexes
  auto gotindexes = index_names_.find(table_name)->second;
  for(auto i = gotindexes.begin();i!=gotindexes.end();i++){
    // drop indexes on the deleted table 
    DropIndex(table_name, i->first);
  } index_names_.erase(table_name);
  }

  table_id_t drop_table_id = table_names_.find(table_name)->second;
  TableInfo *gotTable = tables_.find(drop_table_id)->second;
  gotTable->GetTableHeap()->FreeHeap();
  buffer_pool_manager_->DeletePage(catalog_meta_->GetTableMetaPages()->find(drop_table_id)->second);
  catalog_meta_->GetTableMetaPages()->erase(drop_table_id);
  
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);;
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  heap_->Free(gotTable);
  
  tables_.erase(drop_table_id);
  table_names_.erase(table_name);

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto got = index_names_[table_name].find(index_name);
  if(got == index_names_[table_name].end()){
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t drop_index_id = got->second;
  IndexInfo *gotIndex = indexes_[drop_index_id];
  // first
  gotIndex->GetIndex()->Destroy();
  
  //second
  buffer_pool_manager_->DeletePage(catalog_meta_->GetIndexMetaPages()->find(drop_index_id)->second);
  catalog_meta_->GetIndexMetaPages()->erase(drop_index_id);

  Page * catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  heap_->Free(gotIndex);

  indexes_.erase(drop_index_id);
  index_names_[table_name].erase(index_name);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) return DB_SUCCESS;
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // printf("WTF?\n");
  Page *table_page = buffer_pool_manager_->FetchPage(page_id); // fetch the saved page
  if(table_page == nullptr){
    return DB_FAILED; // File Damaged
  } 
  TableInfo *cur_table = nullptr;
  TableMetadata *cur_table_meta = nullptr;
  TableHeap *cur_table_heap = nullptr;
  cur_table = TableInfo::Create(heap_);
  TableMetadata::DeserializeFrom(table_page->GetData(), cur_table_meta, cur_table->GetMemHeap());

  if(cur_table_meta == nullptr){
    return DB_FAILED; // there is problem with serialization and de
  }

  cur_table_heap = TableHeap::Create(buffer_pool_manager_, (page_id_t)cur_table_meta->GetFirstPageId(), cur_table_meta->GetSchema(), log_manager_, lock_manager_, cur_table->GetMemHeap());
  if(cur_table_heap == nullptr){
    return DB_FAILED;
  }
  
  cur_table->Init(cur_table_meta, cur_table_heap);
  table_names_.emplace(cur_table_meta->GetTableName(), table_id);
  tables_.emplace(table_id, cur_table);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // printf("WTF\n");
  Page *index_page = buffer_pool_manager_->FetchPage(page_id); // fetch the saved page
  if(index_page == nullptr){
    return DB_FAILED; // File Damaged
  } 
  IndexInfo *cur_index = nullptr;
  IndexMetadata *cur_index_meta = nullptr;
  table_id_t cur_table_id;
  std::string cur_table_name;
  cur_index = IndexInfo::Create(heap_);
  IndexMetadata::DeserializeFrom(index_page->GetData(), cur_index_meta, cur_index->GetMemHeap());

  if(cur_index_meta == nullptr){
    return DB_FAILED; // there is problem with serialization and de
  }
  
  cur_table_id = cur_index_meta->GetTableId();
  cur_table_name = tables_.find(cur_table_id)->second->GetTableName();
  cur_index->Init(cur_index_meta, tables_.find(cur_table_id)->second, buffer_pool_manager_);

  index_names_[cur_table_name].emplace(cur_index_meta->GetIndexName(), index_id);
  indexes_.emplace(index_id, cur_index);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;  

}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto got = tables_.find(table_id);
  if(got == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = tables_[table_id];
  return DB_SUCCESS;
}
