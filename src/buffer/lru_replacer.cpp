#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){capacity = num_pages;}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.size() == 0)
    return false;

  auto last = lru_list_.back();
  cache.erase(last);
  lru_list_.pop_back();
  *frame_id = last;

  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto it = cache.find(frame_id);

  if (it == cache.end())
    return ;

  lru_list_.erase(it->second);
  cache.erase(it);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto it = cache.find(frame_id);

  if (it != cache.end())
    return ;

  ASSERT(lru_list_.size() < capacity, "LRU List is Full!");

  lru_list_.emplace_front(frame_id);
  cache[frame_id] = lru_list_.begin();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}