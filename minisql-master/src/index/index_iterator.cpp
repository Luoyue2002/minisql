#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *Buffer_Pool_Manager, Page *page, int index)
    :buffer_pool_manager_(Buffer_Pool_Manager), page_(page), index_(index)
{
    leaf_node_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if(page_ != nullptr){
    buffer_pool_manager_->UnpinPage(leaf_node_->GetPageId(),false);
  }

}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
//    MappingType result =
    return leaf_node_->GetItem(index_);
//  ASSERT(false, "Not implemented yet.");
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
//  ASSERT(false, "Not implemented yet.");
    /// 需要更改节点
    index_ = index_ + 1;
    if(index_ == leaf_node_->GetSize() && leaf_node_->GetNextPageId() != INVALID_PAGE_ID){
      page_id_t next_page_id = leaf_node_->GetNextPageId();

      Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
      if (next_page != nullptr){
        buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
        page_ = next_page;
        index_ = 0;
        leaf_node_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page_->GetData());;

      }
    }
    return *this;


}


INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  bool is_equal= false;
  if(leaf_node_->GetPageId() == itr.leaf_node_->GetPageId() && index_ == itr.index_){
    is_equal = true;
  }
  return is_equal;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  bool not_equal= true;
  if(leaf_node_->GetPageId() == itr.leaf_node_->GetPageId() && index_ == itr.index_){
    not_equal = false;
  }
  return not_equal;
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
