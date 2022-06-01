#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include  <exception>
//#include "glog/logging.h"

///https://blog.csdn.net/u010180372/article/details/122095553 参考教程1
///https://blog.csdn.net/qq_45698833/article/details/121284841 参考教程2
///https://zhuanlan.zhihu.com/p/469663676 参考教程3
///https://github.com/nefu-ljw/database-cmu15445-fall2020/blob/main/src/storage/index/b_plus_tree.cpp 参考教程4
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          root_page_id_(INVALID_PAGE_ID),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  IndexRootsPage *index_root_page=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t root_page_id;
  bool flag = index_root_page->GetRootId(index_id,&root_page_id);
  if(flag==true) root_page_id_ = root_page_id;


}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
}

/*
 * Helper function to decide whether current b+tree is empty
 */

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
//    bool is_empty ;
    if( root_page_id_ == INVALID_PAGE_ID  )
        return true;

    return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
    if (IsEmpty()) {
        return false;
    }
    bool is_exist = false;
    ValueType value{};
    Page *leaf_page = FindLeafPage(key , false);
    if (leaf_page != nullptr){
        LeafPage * leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

        is_exist = leaf_node->Lookup(key,value ,comparator_);

        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

    }
    if(is_exist){
        result.push_back(value);
        return true;
    }
    return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
    if (IsEmpty()) {
        StartNewTree(key, value);
        return true;
    }
    bool  is_exist = InsertIntoLeaf(key, value);
    return is_exist;
//    return false;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {

    root_page_id_ = INVALID_PAGE_ID;
    Page *root_page_ = buffer_pool_manager_->NewPage(root_page_id_);
    if (root_page_ == nullptr) {
        //do nothing
    }

    LeafPage *root_node_ =  reinterpret_cast<LeafPage *>(root_page_->GetData());

    //init
    if(root_node_ != nullptr) {
      /// UpdateRootPageId
      UpdateRootPageId(1);
      root_node_->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      root_node_->Insert(key, value, comparator_);
      buffer_pool_manager_->UnpinPage(root_page_->GetPageId(), true);
      page_id_t parent_page_id_ = root_node_->GetParentPageId();
      if (parent_page_id_ == 0) {
      };
    }

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
    Page *leaf_page_ = FindLeafPage(key, false);
    if (leaf_page_ == nullptr)
    {
        return false;
    }
    LeafPage * leaf_node = reinterpret_cast<LeafPage *>(leaf_page_->GetData());
    page_id_t parent_page_id_ = leaf_node->GetParentPageId();
    if(parent_page_id_ == 0){};//debug
    //find
    ValueType value_temp;
    bool  is_exist =  leaf_node->Lookup(key, value_temp,comparator_);
    if(is_exist){
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
        return false;
    }
    leaf_node->Insert(key, value ,comparator_);
    //check , weather need spilt
    if (leaf_node->GetSize()>= leaf_node->GetMaxSize()){
        ///Split
        LeafPage * leaf_node_new_ = Split(leaf_node);
        InsertIntoParent(leaf_node, leaf_node_new_->KeyAt(0), leaf_node_new_);
        buffer_pool_manager_->UnpinPage(leaf_node_new_->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t split_new_page_id = INVALID_PAGE_ID;
    Page *split_new_page_ = buffer_pool_manager_->NewPage(split_new_page_id);
    if(split_new_page_ == nullptr){
        ///do noting
        /// error "out of memory"
    }
//    N * new_node;
    N * split_new_node = reinterpret_cast<N *>(split_new_page_ ->GetData());
    if(node->IsLeafPage()){
    //重构
        LeafPage*  split_leaf_node_new = reinterpret_cast<LeafPage *>(split_new_page_ ->GetData());
        LeafPage * split_leaf_node_ = reinterpret_cast<LeafPage *>(node);
        split_leaf_node_new->Init( split_new_page_id,split_leaf_node_->GetParentPageId(),leaf_max_size_);
        split_leaf_node_->MoveHalfTo(split_leaf_node_new);
        split_leaf_node_new->SetNextPageId(split_leaf_node_->GetNextPageId());
        split_leaf_node_->SetNextPageId(split_leaf_node_new->GetPageId());
        split_new_node = reinterpret_cast<N *> (split_leaf_node_new);
    }
    if(!node->IsLeafPage()){
        InternalPage * split_internal_node_new = reinterpret_cast<InternalPage *>(split_new_page_ ->GetData());
        InternalPage * split_internal_node_ = reinterpret_cast<InternalPage *>(node);
        split_internal_node_new->Init(split_new_page_id,split_internal_node_->GetParentPageId(),internal_max_size_);
        split_internal_node_->MoveHalfTo(split_internal_node_new , buffer_pool_manager_);
        split_new_node = reinterpret_cast<N *> (split_internal_node_new);
        /// node 被传出去了，还要被使用，后面用到了记得unpin ,现在unpin 似乎也可
        /// unpin 修改了一下，反正现在unpin 不会小于0， 就是这样估计多线程要炸... 修不动了
        buffer_pool_manager_->UnpinPage(split_new_page_id , true);
    }

    return split_new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
    if(old_node->IsRootPage()){
        page_id_t new_root_page_id = INVALID_PAGE_ID;
        Page *new_page_ = buffer_pool_manager_->NewPage(new_root_page_id);
        root_page_id_ = new_root_page_id;
        if (new_page_ == nullptr) {
            throw std::runtime_error("out of memory");
        }
        InternalPage *  new_root_node = reinterpret_cast<InternalPage *>(new_page_->GetData());
        new_root_node->Init(new_root_page_id,INVALID_PAGE_ID, internal_max_size_);
        new_root_node->PopulateNewRoot(old_node->GetPageId() , key,new_node->GetPageId() );
        old_node->SetParentPageId(new_root_page_id);
        new_node->SetParentPageId(new_root_page_id);
        ///UpdateRootPageId
        UpdateRootPageId(0);
        //debug
//        int value1 = new_root_node->ValueAt(0);
//        int value2 = new_root_node->ValueAt(1);
//        if(value1){} ;
//        if(value2){} ;
        buffer_pool_manager_->UnpinPage(new_root_page_id , true);
        return ;
    }
    if(!old_node->IsRootPage())
    {
        page_id_t parent_page_id_ = old_node->GetParentPageId();
        Page *parent_page_ = buffer_pool_manager_->FetchPage(parent_page_id_);
        InternalPage *  parent_node = reinterpret_cast<InternalPage *>(parent_page_->GetData());
        new_node->SetParentPageId(old_node->GetParentPageId());
        int now_size = parent_node->InsertNodeAfter(old_node->GetPageId() , key,new_node->GetPageId());
        if(now_size >parent_node->GetMaxSize()){
            InternalPage *new_split_node = Split(parent_node);
            InsertIntoParent(parent_node, new_split_node->KeyAt(0) , new_split_node);
            ///调用的spilt node 又unpin了一下 , check all unpin 有 22 的后遗症..
            buffer_pool_manager_->UnpinPage(new_split_node->GetPageId() , true);
        }
        if(now_size <= parent_node->GetMaxSize()){

        }
        // else do nothing
        buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);

    }


}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  Page* leaf_page_ = FindLeafPage(key, false);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page_->GetData());
  //delete it
  leaf_node->RemoveAndDeleteRecord(key,comparator_);
  //after delete
  if(leaf_node->GetSize()<leaf_node->GetMinSize()){
    //adjust
//    LeafPage *leaf_node_ = reinterpret_cast<LeafPage *>(leaf_page_->GetData());
    CoalesceOrRedistribute(leaf_node, transaction);
  }
  if(leaf_node->GetSize()>=leaf_node->GetMinSize()){
    //do nothing
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  BPlusTreePage *node_ = reinterpret_cast<BPlusTreePage *>(node);
  if (node_->GetSize() >= node->GetMinSize()) {
    return false;
  }
  if (node_->IsRootPage()) {
    bool  root_adjust = AdjustRoot(node);
    return root_adjust;
  }

  if(!node_->IsRootPage()){
    /// try to find sibling
    Page *parent_page_ = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    InternalPage *parent_node_ = reinterpret_cast<InternalPage *>(parent_page_->GetData());
    int index = parent_node_->ValueIndex(node->GetPageId());
    page_id_t sibling_page_id_ ;
    // got index
    /// 尽量寻找前驱节点
    if(index == 0){
      sibling_page_id_  = parent_node_->ValueAt(1);
    }
    if(index > 0){
      sibling_page_id_  = parent_node_->ValueAt(index - 1);
    }
    // fetch
    Page *sibling_page_ = buffer_pool_manager_->FetchPage(sibling_page_id_);
    N *sibling_node = reinterpret_cast<N *>(sibling_page_->GetData());

    if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()){
      //Redistribute
      Redistribute(sibling_node, node, index);
      buffer_pool_manager_->UnpinPage(parent_page_->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling_page_->GetPageId(), true);
      return false;
      //stop
    }
    if (node->GetSize() + sibling_node->GetSize() < node->GetMaxSize()){
      //Coalesce
      // 注意一下 Coalesce 的参数 N** ,
      Coalesce(&sibling_node, &node, &parent_node_, index);
      buffer_pool_manager_->UnpinPage(parent_page_->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling_page_->GetPageId(), true);
      return true;

    }

  }

  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  BPlusTreePage *node_ = reinterpret_cast<BPlusTreePage *>(*node);
  int key_index = index;
  /// 天坑  这里出了不少问题 key_index 非常有必要  非前驱节点的 middle_key 要传 index = 1 的 key 下来， 此时 index 实际是 0
  /// 我的条件判断又是基于 index 所以 index 不能乱改
  if (index == 0) {
    key_index = 1;
  }

  if(node_->IsLeafPage()){
    LeafPage *leaf_node_ = reinterpret_cast<LeafPage *>(*node);
    LeafPage *leaf_neighbor_node_ = reinterpret_cast<LeafPage *>(*neighbor_node);
    if(index == 0){
      ///前驱合并，我没注意犯了大错，直接爆炸..
      ///把neighbor_node 和 node 交换一下
      LeafPage * temp = nullptr;
      temp = leaf_node_;
      leaf_node_ = leaf_neighbor_node_ ;
      leaf_neighbor_node_ = temp;
      leaf_node_->MoveAllTo(leaf_neighbor_node_);
      leaf_neighbor_node_->SetNextPageId(leaf_node_->GetNextPageId());
    }
    if(index > 0){
      leaf_node_->MoveAllTo(leaf_neighbor_node_);
      leaf_neighbor_node_->SetNextPageId(leaf_node_->GetNextPageId());
    }
  }
  if(!node_->IsLeafPage()){
    KeyType middle_key = (*parent)->KeyAt(key_index);
    InternalPage *internal_node_ = reinterpret_cast<InternalPage *>(*node);
    InternalPage *internal_neighbor_node_ = reinterpret_cast<InternalPage *>(*neighbor_node);
    if(index == 0){
      InternalPage * temp = nullptr;
      temp = internal_node_;
      internal_node_ = internal_neighbor_node_;
      internal_neighbor_node_ = temp;
      internal_node_->MoveAllTo(internal_neighbor_node_, middle_key, buffer_pool_manager_);
    }
    if(index > 0){
      internal_node_->MoveAllTo(internal_neighbor_node_, middle_key, buffer_pool_manager_);
    }
    buffer_pool_manager_->UnpinPage((*node)->GetPageId(),true);
    buffer_pool_manager_->DeletePage((*node)->GetPageId());


  }
  (*parent)->Remove(key_index); /// remove 也要改一下
  bool need_delete = CoalesceOrRedistribute((*parent), nullptr); // 递归调用
  return need_delete;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {

  BPlusTreePage *node_ = reinterpret_cast<BPlusTreePage *>(node);
  Page *parent_page = buffer_pool_manager_->FetchPage(node_->GetParentPageId());
  InternalPage * parent_node_ = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 节点调整在这里进行

  if(node_->IsLeafPage()){
    LeafPage * leaf_node_ = reinterpret_cast<LeafPage *>(node);
    LeafPage * leaf_neighbor_node_ = reinterpret_cast<LeafPage *>(neighbor_node);
    // index
    ///move sibling page's first key & value pair into end of input "node",
    if(index == 0){
      leaf_neighbor_node_->MoveFirstToEndOf(leaf_node_);
      ///set parent
      parent_node_->SetKeyAt(1,leaf_neighbor_node_->KeyAt(0));
      ///index 看意思是要我尽量拿前驱结点，不懂想让我干啥，看起来是为了尽量保持所有节点半满？
      leaf_node_->SetParentPageId(parent_node_->GetPageId());
      leaf_neighbor_node_->SetParentPageId(parent_node_->GetPageId());
      ///index 是 删除过元素的node在parent 处的下标
    }
    ///otherwise move sibling page's last key & value pair into head of input node
    if(index > 0){
      leaf_neighbor_node_->MoveLastToFrontOf(leaf_node_);
      parent_node_->SetKeyAt(index, leaf_node_->KeyAt(0));
      leaf_node_->SetParentPageId(parent_node_->GetPageId());
      leaf_neighbor_node_->SetParentPageId(parent_node_->GetPageId());
    }

  }
  if(!node_->IsLeafPage()){
    InternalPage * internal_node_ = reinterpret_cast<InternalPage *>(node);
    InternalPage * internal_neighbor_node_ = reinterpret_cast<InternalPage *>(neighbor_node);
    if(index == 0){
      internal_neighbor_node_->MoveFirstToEndOf(internal_node_,parent_node_->KeyAt(1),buffer_pool_manager_);
      parent_node_->SetKeyAt(1,internal_neighbor_node_->KeyAt(0));
      internal_node_->SetParentPageId(parent_node_->GetPageId());
      internal_neighbor_node_->SetParentPageId(parent_node_->GetPageId());
    }
    if(index >0){
      internal_neighbor_node_->MoveLastToFrontOf(internal_node_, parent_node_->KeyAt(index), buffer_pool_manager_);
      parent_node_->SetKeyAt(index, internal_node_->KeyAt(0));
      internal_node_->SetParentPageId(parent_node_->GetPageId());
      internal_neighbor_node_->SetParentPageId(parent_node_->GetPageId());
    }
  }
  buffer_pool_manager_->UnpinPage(parent_node_->GetPageId(), true);

}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  //  root page need to be update
  /// 根节点更新
  if(old_root_node->IsRootPage() && old_root_node->GetSize()==1){
    // set its child to be new root
    InternalPage * Internal_old_root_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id_ = Internal_old_root_node->RemoveAndReturnOnlyChild();
    Page *new_root_page = buffer_pool_manager_->FetchPage(child_page_id_);
    InternalPage * new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    // update root id
    root_page_id_ = child_page_id_;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage((old_root_node->GetPageId()), false);
    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId() , true);
    return true;
  }
  /// 清空树
  if(old_root_node->IsLeafPage() && old_root_node->GetSize()==0){
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    return true;

  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key{};
  Page *leaf_page_ = FindLeafPage(key ,true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page * leaf_page_ = FindLeafPage(key );
  LeafPage *leaf_node_ = reinterpret_cast<LeafPage *>(leaf_page_->GetData());
  int index = leaf_node_->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_, index);

}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  /// 遍历叶子
  KeyType key{};
  Page * leaf_page_ = FindLeafPage(key, true);
  LeafPage * leaf_node_= reinterpret_cast<LeafPage *>( leaf_page_->GetData());
  page_id_t next_node_page_id_ = leaf_node_->GetNextPageId() ;
  while(next_node_page_id_!=INVALID_PAGE_ID){
    next_node_page_id_=leaf_node_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_node_->GetPageId(),false);
    Page * next_node_page = buffer_pool_manager_->FetchPage(next_node_page_id_);
    leaf_page_ = next_node_page;
    leaf_node_=reinterpret_cast<LeafPage *>(leaf_page_->GetData() );
    next_node_page_id_ = leaf_node_->GetNextPageId() ;
  }

  return INDEXITERATOR_TYPE(buffer_pool_manager_,leaf_page_,leaf_node_->GetSize());
  /// 迭代器的end 是要指向最后之后的
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
    if (IsEmpty()) {
        return nullptr;
    }
    // first try to get the subtree node of root, fetch root to get
    auto page_now_ = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *node_now_ = reinterpret_cast<BPlusTreePage *>(page_now_->GetData());

    while( !node_now_->IsLeafPage()){
//        auto  Internal_node_now_ = reinterpret_cast<BPlusTreeInternalPage <KeyType, page_id_t,KeyComparator> *>(page_now_->GetData());
        auto  Internal_node_now_ = reinterpret_cast<InternalPage *>(page_now_->GetData());
        page_id_t child_node_page_id_;
        if(leftMost){
            child_node_page_id_ = Internal_node_now_->ValueAt(0);

        }
        else{
            child_node_page_id_ = Internal_node_now_->Lookup(key, comparator_);
        }
        auto child_page_now_ = buffer_pool_manager_->FetchPage(child_node_page_id_);
        auto child_node_now_ = reinterpret_cast<BPlusTreePage *>(child_page_now_->GetData());
        node_now_ = child_node_now_;
        page_now_ = child_page_now_;

        //unpin it
        buffer_pool_manager_->UnpinPage( Internal_node_now_->GetPageId(), false);

    }

    auto node_now = reinterpret_cast<LeafPage *>(page_now_->GetData());
    page_id_t parent_page_id_ = node_now->GetParentPageId();
    if(parent_page_id_ == 0){};

    return page_now_;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
    Page *root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    IndexRootsPage *root_node  = reinterpret_cast<IndexRootsPage *>(root_page->GetData());
    if(insert_record == 1){
        root_node->Insert( index_id_ , root_page_id_);
    }
    else{
        root_node->Update( index_id_ , root_page_id_);
    }

    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID , true);

}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
