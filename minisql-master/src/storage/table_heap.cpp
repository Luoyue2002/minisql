#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
//  TablePage* page;
//  page_id_t page_id = first_page_id_;
//  bool re;
//  while( (page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id)) ) != nullptr ) {
//    // how to make sure that whether the page can hold the row
//    /// need to find whether hold by the return value of TablePage::InsertTuple
//    page->WLatch();
//    re = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
//    page->WUnlatch();
//    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//    if(re) return true;
//    page_id = page->GetNextPageId();
//    if(page_id == INVALID_PAGE_ID){
//      break;
//    }
//  }
//
//  // if(page == nullptr) {
//  /// the page must be nullptr to get out of the loop
//  page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id) );
//  if (!page) return false;
//
//  // how to insert data to the page
//  /// TablePage::InsertTuple
//  re = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
//
//  return re;
    Page * page_now = buffer_pool_manager_->FetchPage(first_page_id_);
    TablePage * table_page_now = reinterpret_cast<TablePage *>(page_now);
    if(table_page_now != nullptr){
      bool  finish = table_page_now->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      while (!finish) {
        page_id_t next_page_id = table_page_now->GetNextPageId();

        if (next_page_id != INVALID_PAGE_ID) {

          buffer_pool_manager_->UnpinPage(table_page_now->GetTablePageId(), false);
          table_page_now = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));

        } else {

          TablePage * new_table_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage( next_page_id));
          if (new_table_page == nullptr) {

            buffer_pool_manager_->UnpinPage(table_page_now->GetTablePageId(), false);
            return false;
          }

          table_page_now->SetNextPageId(next_page_id);
          table_page_now->Init(next_page_id,table_page_now->GetTablePageId(), log_manager_, txn);
          table_page_now->WUnlatch();
          buffer_pool_manager_->UnpinPage(table_page_now->GetTablePageId(), true);
          table_page_now = new_table_page;
          finish = table_page_now->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        }
      }


      buffer_pool_manager_->UnpinPage(table_page_now->GetTablePageId(), true);

      return true;
    }


    else{
      return false ;
    }

    return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }

  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/*
  Update the tuple
*/
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  bool re = MarkDelete(rid, txn);
  if(!re) // state 1
    return false;

  re = InsertTuple(row, txn);
  if(!re) // state 2
    return false;

  return true;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.

  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) return;

  // delete the tuple physically
  page->ApplyDelete(rid, txn, log_manager_);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  TablePage* page;
  page_id_t page_id;

  
  while( (page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_)) ) != nullptr ) {
    // how to make sure that whether the page can hold the row
    /// need to find whether hold by the return value of TablePage::InsertTuple
    page_id = page->GetNextPageId();
    buffer_pool_manager_->DeletePage(first_page_id_);
    first_page_id_ = page_id;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  TablePage* page;
  page_id_t page_id = first_page_id_;
  bool re;
  while( (page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id)) ) != nullptr ) {
    // how to make sure that whether the page can hold the row
    /// need to find whether hold by the return value of TablePage::InsertTuple
    page->RLatch();
    re = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);

    if(re) return true;
    page_id = page->GetNextPageId();
  }

  return false;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  RowId rowid;
  TablePage* page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page->GetFirstTupleRid(&rowid);
  return TableIterator( page, rowid, schema_, buffer_pool_manager_ );
}

TableIterator TableHeap::End() {
  TablePage* page, *page2;
  page_id_t page_id = first_page_id_;
  
  while( (page2 = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id)) ) != nullptr ) {
    // how to make sure that whether the page can hold the row
    /// need to find whether hold by the return value of TablePage::InsertTuple
    page = page2;
    page_id = page->GetNextPageId();
  }

  // find the tuple
  RowId rowid, rowid2;
  page->GetFirstTupleRid(&rowid);
  while(page->GetNextTupleRid(rowid, &rowid2)) rowid = rowid2;

  // return the iterator
  return TableIterator( page, rowid, schema_, buffer_pool_manager_ );
}
