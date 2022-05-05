#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}
//参考文章
//https://blog.csdn.net/twentyonepilots/article/details/120868216
//https://github.com/nefu-ljw/database-cmu15445-fall2020
//std::scoped_lock lock{latch_}; 并发锁

/*
 * frame和page、buffer pool和replacer之间的关系
 * buffer pool像一个page的容器，而page在容器的哪里由frame标记，page table就是记录page和frame的映射关系的。
 * 也就是说，frame是一个物理的概念。而frame中，哪些被page占用，哪些是空的，由free_list标记。
 * 哪些包含page的frame可以被victim，需要借助replacer来选择。replacer仅仅记录frame的编号。
 * 最后，page中的page_id说明它来自于哪个物理页面，
 * 新建页面是要pin的，而fetch页面时如果已存在则引用计数加一，否则相当于新建一个页面，引用计数设为1
 */

/*        Page对象并不作用于唯一的数据页，它只是一个用于存放从磁盘中读取的数据页的容器。
        这也就意味着同一个Page对象在系统的整个生命周期内，可能会对应很多不同的物理页。
        所以 pages_ 的追踪标志是 frame id 不是 page id ！！！
        page id 是物理页对应 ！！
*/

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    std::scoped_lock lock{latch_};
  if (page_table_.find(page_id) != page_table_.end()){ //在page table 里
      Page *page_now_ = &pages_[page_table_.find(page_id)->second];
      page_now_->pin_count_ = page_now_->pin_count_ + 1;
      replacer_->Pin(page_table_.find(page_id)->second);
      return  page_now_ ;
  }
//  if (page_table_.find(page_id) == page_table_.end())
  {//不在page table 里
//      Page *page_now_ = NewPage(page_id);  // 这么写不行，我忘了我在NewPage 里 memset 了 ，fetch 要 read disk manager 的
        // 找不到，用 replacer 搞一个
      frame_id_t frame_id;
      
      if(!free_list_.empty()){
          frame_id = free_list_.front();
          free_list_.pop_front();
      }
      if(free_list_.empty()){
          replacer_->Victim(&frame_id );
          // return nullptr;
      }

      Page *page_now_ = &pages_[frame_id];
      if (page_now_->is_dirty_ == true){
          disk_manager_->WritePage(page_now_ ->page_id_, page_now_ ->data_);
          page_now_->is_dirty_ = false;
      }

      page_table_.erase(page_now_ ->page_id_);
      page_table_.emplace(page_id, frame_id);

      page_now_->page_id_ =page_id;
      page_now_->pin_count_ = 1; //pin

      replacer_->Pin(frame_id);
      disk_manager_->ReadPage(page_id, page_now_->data_);
      return page_now_;

  }

}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
    std::scoped_lock lock{latch_};
    // find in the buffer_pool

    bool  have_page = false;
    for(size_t id = 0 ; id < pool_size_; id++ ){
        Page *page_now_ = &pages_[id];
        if( page_now_->pin_count_ > 0 ) {// could not use
            have_page = false;
        } else{
            have_page = true;
            break;
        }
    }

    if (have_page == false){
        return nullptr;
    }

    {
        frame_id_t frame_id;
        //find victim page from free list
        if(!free_list_.empty()){
            frame_id = free_list_.front();
            free_list_.pop_front();
        }
        if(free_list_.empty()){
            replacer_->Victim(&frame_id );
            // nullptr
        }


        // update
        page_id = disk_manager_->AllocatePage();
        Page *page_now_ = &pages_[frame_id];
        if (page_now_->is_dirty_ == true){
            disk_manager_->WritePage(page_now_ ->page_id_, page_now_ ->data_);
            page_now_->is_dirty_ = false;
        }
        /// 测试一直过不去。后来发现是这里的问题 create new 后 page table 要更新，给忘了
        page_table_.erase(page_now_->page_id_);

        page_now_->is_dirty_ = false;
        page_now_->page_id_ =page_id;

        page_now_->pin_count_ = 1; //pin

        memset(page_now_->data_ ,0, PAGE_SIZE);
        page_table_.emplace(page_id, frame_id);



        replacer_->Pin(frame_id);

        page_table_.emplace(page_id, frame_id);
        //add P to the page table.
        return page_now_;
    }

}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    std::scoped_lock lock{latch_};
    if(page_table_.find(page_id) == page_table_.end()){
        return true;
    }
    if (page_table_.find(page_id) != page_table_.end()){
        Page *page_now_ = &pages_[page_table_.find(page_id)->second];
        frame_id_t frame_id = page_table_.find(page_id)->second;
        if (page_now_->pin_count_ >= 0){
            return false;
        }
        if (page_now_->is_dirty_ == true){
            disk_manager_->WritePage(page_now_ ->page_id_, page_now_ ->data_);
            page_now_ ->is_dirty_ = false;
        }
        page_table_.erase(page_id); //
        disk_manager_->DeAllocatePage(page_id);
        //reset
        page_now_->pin_count_ = 0;
        page_now_->is_dirty_ = false;
        page_now_->page_id_ = INVALID_PAGE_ID;
//        page_now_->ResetMemory();
        memset(page_now_->data_ ,0, PAGE_SIZE);

        free_list_.push_back(frame_id);
    }

  return false;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::scoped_lock lock{latch_};
    // is dirty 没搞明白用来干啥
    if(page_table_.find(page_id) != page_table_.end()){
        Page *page_now_ = &pages_[page_table_.find(page_id)->second];


        page_now_ ->pin_count_ = page_now_ ->pin_count_ - 1 ;
        if(page_now_ ->pin_count_  < 0 ){
            page_now_ ->pin_count_ = page_now_ ->pin_count_ + 1 ;
            return false;
        }

        if(page_now_ ->pin_count_  == 0){
            replacer_->Unpin(page_table_.find(page_id)->second);
//            return true;
        }

        return true;

    }
//    if(page_table_.find(page_id) == page_table_.end())
    return true;

}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    if(page_table_.find(page_id) != page_table_.end() && (page_id != INVALID_PAGE_ID) ){
//        Page *page_now_ = &pages_[page_id];

        Page *page_now_ = &pages_[page_table_.find(page_id)->second];
        disk_manager_->WritePage(page_now_ ->page_id_, page_now_ ->data_);
        page_now_ ->is_dirty_ = false;
        return true;
    }else{
        return false;
    }

}
// FlushAllPages 没有给接口，自己补一个
bool BufferPoolManager::FlushAllPages(){
    // get pool size
    std::scoped_lock lock{latch_};
    for(size_t id = 0 ; id < pool_size_; id++ ){
        Page *page_now_ = &pages_[id];
        if( (page_now_->is_dirty_ == true) && (page_now_->page_id_ != INVALID_PAGE_ID) ){
            disk_manager_->WritePage(page_now_->page_id_, page_now_->data_);
            page_now_->is_dirty_ = false;
        }
    }
    return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
