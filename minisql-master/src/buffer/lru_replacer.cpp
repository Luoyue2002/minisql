#include "buffer/lru_replacer.h"

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
//    if (hashmap.count(frame_id) == 0)
    if (hashmap.find(frame_id) == hashmap.end())
    {
        return;
    }
    //release
    frame_id_lists.erase(hashmap[frame_id]);
    hashmap.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    if(frame_id_lists.size()>= size_of_LRUreplacer)
        return;
//    if (hashmap.count(frame_id) == 0)
    if (hashmap.find(frame_id) == hashmap.end())
    {

        frame_id_lists.push_front(frame_id);
        hashmap[frame_id] = frame_id_lists.begin();
    }
}

size_t LRUReplacer::Size() {
  return frame_id_lists.size();
}