#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    //init
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetMaxSize(max_size);
    SetPageId(page_id);
    SetParentPageId(parent_id);
//    MappingType array_[0];
//    memset(array_ , 0 ,sizeof(array_));

}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
  // I am not sure is this right
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    array_[index].first = key;
    return;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    //find
    int i;
    for(i=0 ; i< GetSize(); i++){
        if(array_[i].second == value){
            return i;
        }
    }

    return -1; // not found
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  ValueType val = array_[index].second;
  return val;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */

INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
    ///Start the search from the second key(the first key should always be invalid)
//    ValueType val{};
//    if(comparator(key, array_[1].first) <0 ){
//      val = array_[0].second;
//        return val;
//    }
//    if(comparator(key, array_[GetSize()-1].first) >=0 ){
//        val = array_[GetSize()-1].second;
//        return val;
//    }
//    for(int key_index = 1 ; key_index < GetSize(); key_index++){
//        //key_index < GetSize()-1 this because if key_index < GetSize() ,index+1 does not exist
//        if ((comparator(key, array_[key_index].first) < 0)){
//            //find
//            val = array_[key_index-1].second;
//            break ;
//
//        }
//
//
//    }
    ValueType val{};
    int size = GetSize();
    int left = 1;
    int right = size - 1;
    while (left <= right) {
      int mid = (right + left) / 2;
      if (comparator(KeyAt(mid),key)>0) {
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }
    val = array_[left-1].second;
    return val;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
    array_[0].second = old_value;
    array_[1].first = new_key;
    array_[1].second = new_value;
    IncreaseSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
    int old_value_index = ValueIndex(old_value);

    for(int i=GetSize() ; i>old_value_index  ; i--){
        array_[i].first = array_[i-1].first;
        array_[i].second = array_[i-1].second;
    }
    array_[old_value_index + 1].first = new_key;
    array_[old_value_index + 1].second = new_value;
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
    int half_size = GetMinSize();
    int size = GetSize();

    recipient ->CopyNFrom( array_ + half_size  ,  size - half_size, buffer_pool_manager); // 注意奇数偶数
    IncreaseSize(-1*(size - half_size));



}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    /// me 看了半天才反映过来是CopyNForm
    int  array_size= GetSize();
    for(int i = 0 ; i < size ; i++ ){
        array_[i+array_size].first = items[i].first;
        array_[i+array_size].second = items[i].second;

        /// changing their parent page id, which needs to be persisted with BufferPoolManger
        auto child_page = buffer_pool_manager->FetchPage(array_[i+array_size].second);
//        if (child_page != nullptr)
        {
            auto child_node = reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
            child_node->SetParentPageId(GetPageId());
            buffer_pool_manager->UnpinPage(array_[i+array_size].second, true);
        }


    }
    IncreaseSize(size);

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    for (int i = index; i < GetSize() - 1; i++) {
        array_[i].first = array_[i + 1].first;
        array_[i].second = array_[i + 1].second;
    }
    IncreaseSize(-1);

}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
    ValueType val = array_[0].second;
    SetSize(0);
    return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  //get parent page
    Page* parent_page_ = buffer_pool_manager->FetchPage(GetParentPageId());
    BPlusTreeInternalPage *parent_node_ = reinterpret_cast<BPlusTreeInternalPage *>(parent_page_->GetData());
    SetKeyAt(0, middle_key);

    buffer_pool_manager->UnpinPage(parent_node_->GetPageId(), true);
//    int size = GetSize();
    recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
    for (auto i = 0; i < GetSize(); i++)
    {
      auto page = buffer_pool_manager->FetchPage(array_[i].second);
      BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      node->SetParentPageId(recipient->GetPageId());

      buffer_pool_manager->UnpinPage(node->GetPageId(), true);
    }

    SetSize(0);


}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
    SetKeyAt(0,middle_key);
    recipient->CopyLastFrom(array_[0], buffer_pool_manager);
//    Page *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
//    BPlusTreeInternalPage *parent_node =  reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
//    parent_node->

    for (int i = 0; i < GetSize() - 1; i++) {
      array_[i].first = array_[i + 1].first;
      array_[i].second = array_[i + 1].second;
    }
    //or
//    remove(0);
    IncreaseSize(-1);




}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    array_[size].first = pair.first;
    array_[size].second = pair.second;

    auto child_page = buffer_pool_manager->FetchPage(array_[GetSize()].second);
    if (child_page != nullptr) {
        auto *child_node = reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
        child_node->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(array_[GetSize()].second, true);
    }
    IncreaseSize(1);

}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(array_[GetSize() - 1], buffer_pool_manager);
  IncreaseSize(-1);


}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
//    Page *page_now_ = buffer_pool_manager ->FetchPage(GetParentPageId());
    int i;
    for (i = GetSize(); i > 0; i--) {
        array_[i].first = array_[i - 1].first;
        array_[i].second = array_[i - 1].second;
    }
    array_[0].first = pair.first;
    array_[0].second = pair.second;
    IncreaseSize(1);
    auto child_page = buffer_pool_manager->FetchPage(array_[0].second);
    if (child_page != nullptr) {
        auto *child_node = reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
        child_node->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(array_[0].second, true);
    }

}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

template
class BPlusTreeInternalPage<GenericKey<128>, page_id_t, GenericComparator<128>>;

template
class BPlusTreeInternalPage<GenericKey<256>, page_id_t, GenericComparator<256>>;
