#include "buffer/lru_replacer.h"


//利用链表和hashmap。当需要插入新的数据项的时候，如果新数据项在链表中存在（一般称为命中）
// 则把该节点移到链表头部，如果不存在，则新建一个节点，放到链表头部，
// 若缓存满了，则把链表最后一个节点删除即可。
// 在访问数据的时候，如果数据项在链表中存在，则把该节点移到链表头部，否则返回-1。
// 这样一来在链表尾部的节点就是最近最久未访问的数据项。

LRUReplacer::LRUReplacer(size_t num_pages) {
    size_of_LRUreplacer = num_pages ;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if (frame_id_lists.empty())
        return false;
    //give the last to frame_id,
    *frame_id = frame_id_lists.back();
    //delete
    hashmap.erase(*frame_id);
    frame_id_lists.pop_back();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
//    if(hashmap.find(frame_id)!=hashmap.end()){   // find
//        return;
//    }
    if (hashmap.count(frame_id) == 0) {
        return;
    }
    //release
    frame_id_lists.erase(hashmap[frame_id]);
    hashmap.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {

    if(frame_id_lists.size()>= size_of_LRUreplacer)
        return;
    if (hashmap.count(frame_id) == 0) {
        // 若不在，由于当前页面的引用为0，则把它加入到待置换的 list 头部中。
        frame_id_lists.push_front(frame_id);
        hashmap[frame_id] = frame_id_lists.begin();
    }
//    if (hashmap.find(frame_id)==hashmap.end()){
//        frame_id_lists.push_front(frame_id);
//        hashmap[frame_id] = frame_id_lists.begin();
//
//    }


}

size_t LRUReplacer::Size() {

  return frame_id_lists.size();
}