#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  // add your own private member variables here
  
  /*
   about lru
   use list and hashmap
   when we need to insert new data
   if it exists in list ,move to list head\
   if not, creat and move to list head，
   if full delete the last

   when search,
   if exists move to list head
   else return -1
   */

  std::list<frame_id_t> frame_id_lists; // this is used to store
  //hash map std::unordered
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> hashmap;
  //size
  size_t size_of_LRUreplacer ;
};

#endif  // MINISQL_LRU_REPLACER_H
