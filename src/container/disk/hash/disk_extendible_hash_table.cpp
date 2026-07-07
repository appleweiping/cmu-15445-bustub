//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // Allocate and initialise the single header page for this index.
  index_name_ = name;
  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto *header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash = Hash(key);

  // header -> directory -> bucket, releasing each guard as we descend.
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto *header = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t dir_idx = header->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = header->GetDirectoryPageId(dir_idx);
  header_guard.Drop();
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard dir_guard = bpm_->FetchPageRead(dir_page_id);
  auto *directory = dir_guard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  dir_guard.Drop();
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto *bucket = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  if (bucket->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t dir_idx = header->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = header->GetDirectoryPageId(dir_idx);

  // No directory yet for this slot: create one (and its first bucket).
  if (dir_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, dir_idx, hash, key, value);
  }
  header_guard.Drop();

  WritePageGuard dir_guard = bpm_->FetchPageWrite(dir_page_id);
  auto *directory = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }

  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto *bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // Duplicate key: reject (unique-key semantics for Fall 2023).
  V existing;
  if (bucket->Lookup(key, existing, cmp_)) {
    return false;
  }

  // Fast path: bucket has room.
  if (!bucket->IsFull()) {
    return bucket->Insert(key, value, cmp_);
  }

  // Bucket full: split (possibly growing the directory) until the key fits.
  while (bucket->IsFull()) {
    uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
    // If local depth would exceed global depth, grow the directory first.
    if (local_depth == directory->GetGlobalDepth()) {
      if (directory->GetGlobalDepth() >= directory->GetMaxDepth()) {
        return false;  // cannot grow any further
      }
      directory->IncrGlobalDepth();
    }

    // Allocate the split-image bucket and bump both local depths.
    page_id_t new_bucket_page_id;
    BasicPageGuard new_bucket_basic = bpm_->NewPageGuarded(&new_bucket_page_id);
    if (new_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    auto *new_bucket = new_bucket_basic.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);

    uint32_t new_local_depth = local_depth + 1;
    uint32_t local_depth_mask = (1U << new_local_depth) - 1;
    // The split image index has the new high bit set.
    uint32_t split_idx = bucket_idx | (1U << local_depth);
    UpdateDirectoryMapping(directory, split_idx, new_bucket_page_id, new_local_depth, local_depth_mask);

    // Redistribute the old bucket's entries between it and its split image.
    MigrateEntries(bucket, new_bucket, split_idx, local_depth_mask);

    // Recompute which bucket the key now maps to and continue if still full.
    bucket_idx = directory->HashToBucketIndex(hash);
    page_id_t target_page_id = directory->GetBucketPageId(bucket_idx);
    if (target_page_id == new_bucket_page_id) {
      bucket_guard = std::move(new_bucket_basic).UpgradeWrite();
      bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    }
    // else: keep operating on the original `bucket`/`bucket_guard`.
  }

  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t dir_page_id;
  BasicPageGuard dir_guard = bpm_->NewPageGuarded(&dir_page_id);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto *directory = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, dir_page_id);

  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  return InsertToNewBucket(directory, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  BasicPageGuard bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto *bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // Every directory slot whose low `new_local_depth` bits match the split image
  // now points to the new bucket; the rest keep the old bucket. Both groups get
  // the incremented local depth.
  uint32_t suffix = new_bucket_idx & local_depth_mask;
  for (uint32_t i = 0; i < directory->Size(); i++) {
    if ((i & local_depth_mask) == suffix) {
      directory->SetBucketPageId(i, new_bucket_page_id);
      directory->SetLocalDepth(i, new_local_depth);
    } else if ((i & (local_depth_mask >> 1)) == (suffix & (local_depth_mask >> 1))) {
      // Sibling slots that shared the original bucket also bump their depth.
      directory->SetLocalDepth(i, new_local_depth);
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  // Walk the old bucket; entries whose hash low-bits match the new bucket's
  // suffix move to the new bucket, the rest stay. Iterate from the back so the
  // in-place RemoveAt compaction does not skip elements.
  uint32_t suffix = new_bucket_idx & local_depth_mask;
  for (int i = static_cast<int>(old_bucket->Size()) - 1; i >= 0; i--) {
    const auto &entry = old_bucket->EntryAt(i);
    uint32_t h = Hash(entry.first);
    if ((h & local_depth_mask) == suffix) {
      new_bucket->Insert(entry.first, entry.second, cmp_);
      old_bucket->RemoveAt(i);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t dir_idx = header->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = header->GetDirectoryPageId(dir_idx);
  header_guard.Drop();
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard dir_guard = bpm_->FetchPageWrite(dir_page_id);
  auto *directory = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  {
    WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto *bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket->Remove(key, cmp_)) {
      return false;
    }
  }

  // Attempt to merge empty buckets with their split images and shrink the
  // directory while the invariants allow it.
  while (true) {
    uint32_t idx = directory->HashToBucketIndex(hash);
    uint32_t local_depth = directory->GetLocalDepth(idx);
    if (local_depth == 0) {
      break;
    }
    uint32_t split_idx = directory->GetSplitImageIndex(idx);
    // Split image must share the same local depth to be mergeable.
    if (directory->GetLocalDepth(split_idx) != local_depth) {
      break;
    }

    page_id_t cur_page_id = directory->GetBucketPageId(idx);
    page_id_t split_page_id = directory->GetBucketPageId(split_idx);

    bool cur_empty;
    bool split_empty;
    {
      ReadPageGuard cur_guard = bpm_->FetchPageRead(cur_page_id);
      cur_empty = cur_guard.As<ExtendibleHTableBucketPage<K, V, KC>>()->IsEmpty();
      ReadPageGuard split_guard = bpm_->FetchPageRead(split_page_id);
      split_empty = split_guard.As<ExtendibleHTableBucketPage<K, V, KC>>()->IsEmpty();
    }
    if (!cur_empty && !split_empty) {
      break;
    }

    // Merge: keep the non-empty page (or `cur` if both empty) and point every
    // slot for the pair at it, decrementing their local depths.
    page_id_t keep_page_id = cur_empty ? split_page_id : cur_page_id;
    page_id_t drop_page_id = cur_empty ? cur_page_id : split_page_id;
    uint32_t new_local_depth = local_depth - 1;
    uint32_t merged_mask = (new_local_depth == 0) ? 0U : ((1U << new_local_depth) - 1);
    uint32_t suffix = idx & merged_mask;
    for (uint32_t i = 0; i < directory->Size(); i++) {
      if ((i & merged_mask) == suffix) {
        directory->SetBucketPageId(i, keep_page_id);
        directory->SetLocalDepth(i, new_local_depth);
      }
    }
    if (drop_page_id != keep_page_id) {
      bpm_->DeletePage(drop_page_id);
    }

    if (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    } else {
      // No further shrink possible; another round may still merge a sibling.
      if (new_local_depth == 0) {
        break;
      }
    }
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
