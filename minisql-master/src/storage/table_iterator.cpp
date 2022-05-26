#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

// #include <iostream>
// using namespace std;

TableIterator::TableIterator(TablePage * page, RowId rowid, Schema* schema, BufferPoolManager* buffer_pool_manager) {
  page_ = page;
  row_id_ = rowid;
  schema_ = schema;
  buffer_pool_manager_ = buffer_pool_manager;

  row = new Row(row_id_);
}

TableIterator::TableIterator(const TableIterator &other) {
  page_ = other.page_;
  row_id_ = other.row_id_;
  schema_ = other.schema_;
  buffer_pool_manager_ = other.buffer_pool_manager_;

  row = new Row(row_id_);
}

TableIterator::~TableIterator() {
  delete row;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(page_ == itr.page_ && row_id_ == itr.row_id_) return true;
  else return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if(page_ == itr.page_ && row_id_ == itr.row_id_) return false;
  else return true;
}

const Row &TableIterator::operator*() {
  row->SetRowId(row_id_);
  page_->GetTuple(row, schema_, nullptr, nullptr);
  return *row;
}

Row *TableIterator::operator->() {
  // cout << "table_iterator.cpp1 " << row_id_.GetPageId() << " " << row_id_.GetSlotNum() << endl;
  row->SetRowId(row_id_);
  // cout << "table_iterator.cpp2 " << row->GetRowId().GetPageId() << " " << row->GetRowId().GetSlotNum() << endl;
  page_->RLatch();
  page_->GetTuple(row, schema_, nullptr, nullptr);
  page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_->GetTablePageId(), true);
  return row;
}

TableIterator &TableIterator::operator++() {
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
}

TableIterator& TableIterator::operator++(int) {
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
}
