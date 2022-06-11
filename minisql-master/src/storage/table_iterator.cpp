#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

// #include <iostream>
// using namespace std;

TableIterator::TableIterator(){
  table_heap_ = nullptr;
  row_ = nullptr;
}

TableIterator::TableIterator(TableHeap *table_heap, RowId rid) {
  table_heap_ = table_heap;
  row_ = new Row(rid);
  if (row_->GetRowId().GetPageId() != INVALID_PAGE_ID) {
    table_heap_->GetTuple(row_,nullptr);
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  row_ = new Row(other.row_->GetRowId());
  this->table_heap_ = other.table_heap_;
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row_->GetRowId().Get() == itr.row_->GetRowId().Get();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return row_->GetRowId().Get() != itr.row_->GetRowId().Get();
}

Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator++() {
  /*
  bool re;
  RowId next_id;
  re = page_->GetNextTupleRid(row_id_, &next_id);
  if (re) {
    row_id_ = next_id;
  }
  else {
    page_id_t id = page_->GetNextPageId();
    page_ = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(id));
    page_->GetFirstTupleRid(&row_id_);
  }

  return *this;
  */
  BufferPoolManager *buffer_pool_manager=table_heap_->buffer_pool_manager_;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(row_->GetRowId().GetPageId())); 
  buffer_pool_manager->UnpinPage(row_->GetRowId().GetPageId(), false);
  if(page == nullptr){
    row_->SetRowId(INVALID_ROWID);
    return *this;
  }
  RowId NextId;
  bool re = page->GetNextTupleRid(row_->GetRowId(),&NextId);
  if(!re){
    // go to next page
    bool res = false;
    while(page->GetNextPageId()!=INVALID_PAGE_ID){
      auto next_page=reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(page->GetNextPageId()));
      buffer_pool_manager->UnpinPage(page->GetTablePageId(),false);
      page=next_page;
      if(page->GetFirstTupleRid(&NextId)){
        res = true;
        break;
      }
    }
    if (!res) {
      NextId.Set(INVALID_PAGE_ID, 0);
    }
  }
  delete row_;
  row_= new Row(NextId);
  if( NextId.GetPageId() != INVALID_PAGE_ID){
    table_heap_->GetTuple(row_,nullptr);
  }

  return *this;
}

TableIterator& TableIterator::operator++(int) {
  this->operator++();
  return *this;
}
