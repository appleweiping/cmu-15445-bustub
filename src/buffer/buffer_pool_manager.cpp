//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// Helper (latch must be held): obtain a usable frame from the free list first,
// otherwise evict one from the replacer, flushing it to disk if dirty and
// removing its page-table entry. Returns false if no frame is available.
auto BufferPoolManager::GetAvailableFrame(frame_id_t *out_frame_id) -> bool {
  if (!free_list_.empty()) {
    *out_frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  frame_id_t victim;
  if (!replacer_->Evict(&victim)) {
    return false;
  }
  Page &victim_page = pages_[victim];
  if (victim_page.is_dirty_) {
    FlushFrame(victim);
  }
  page_table_.erase(victim_page.page_id_);
  *out_frame_id = victim;
  return true;
}

// Helper (latch must be held): synchronously write a frame's page to disk via
// the disk scheduler and clear its dirty flag.
void BufferPoolManager::FlushFrame(frame_id_t frame_id) {
  Page &page = pages_[frame_id];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({/*is_write=*/true, page.data_, page.page_id_, std::move(promise)});
  future.get();
  page.is_dirty_ = false;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }

  page_id_t new_id = AllocatePage();
  Page &page = pages_[frame_id];
  page.ResetMemory();
  page.page_id_ = new_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  page_table_[new_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  *page_id = new_id;
  return &page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);

  // Fast path: the page is already resident. Pin it and record the access.
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];
    page.pin_count_++;
    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);
    return &page;
  }

  // Slow path: bring the page in from disk into a newly acquired frame.
  frame_id_t frame_id;
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }
  Page &page = pages_[frame_id];
  page.ResetMemory();
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({/*is_write=*/false, page.data_, page_id, std::move(promise)});
  future.get();

  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  return &page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];
  if (page.pin_count_ <= 0) {
    return false;
  }
  page.pin_count_--;
  if (is_dirty) {
    page.is_dirty_ = true;
  }
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  FlushFrame(it->second);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> guard(latch_);
  for (auto &[pid, frame_id] : page_table_) {
    FlushFrame(frame_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }

  page_table_.erase(it);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
