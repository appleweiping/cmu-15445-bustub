//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  bool found = false;
  frame_id_t victim = 0;
  size_t victim_dist = 0;
  size_t victim_ts = 0;

  // Choose the evictable frame with the largest backward k-distance. Ties among
  // frames with +inf distance (fewer than k accesses) are broken by classical
  // LRU: evict the one whose earliest access is oldest.
  for (auto &[fid, node] : node_store_) {
    if (!node.IsEvictable()) {
      continue;
    }
    size_t dist = node.BackwardKDistance(current_timestamp_);
    size_t earliest = node.EarliestTimestamp();
    if (!found || dist > victim_dist || (dist == victim_dist && earliest < victim_ts)) {
      found = true;
      victim = fid;
      victim_dist = dist;
      victim_ts = earliest;
    }
  }

  if (!found) {
    return false;
  }
  node_store_.erase(victim);
  curr_size_--;
  *frame_id = victim;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame_id out of range");
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    it = node_store_.emplace(frame_id, LRUKNode(frame_id, k_)).first;
  }
  it->second.RecordAccess(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  bool was_evictable = it->second.IsEvictable();
  if (was_evictable == set_evictable) {
    return;
  }
  it->second.SetEvictable(set_evictable);
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  BUSTUB_ASSERT(it->second.IsEvictable(), "Remove called on a non-evictable frame");
  node_store_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
