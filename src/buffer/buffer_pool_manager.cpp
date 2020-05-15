//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  latch_.lock();  // TODO could use a scoped lock to reduce the chance of not having an unlock call
  frame_id_t free_frame_id = -1;
  if (page_table_.find(page_id) != page_table_.end()) {
    free_frame_id = page_table_[page_id];
    replacer_->Pin(free_frame_id);
    pages_[free_frame_id].pin_count_ += 1;
    latch_.unlock();
    return &pages_[free_frame_id];
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&free_frame_id)) {
      latch_.unlock();
      return nullptr;
    }
    // 2.     If R is dirty, write it back to the disk.
    Page *replacedPage = &pages_[free_frame_id];
    if (replacedPage->is_dirty_) {
      FlushPageImpl(replacedPage->page_id_);
      replacedPage->is_dirty_ = false;
    }
    // 3.     Delete R from the page table and insert P.
    page_table_.erase(replacedPage->page_id_);
  }

  // 4   read in the page content from disk
  pages_[free_frame_id].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[free_frame_id].GetData());

  // 5  Update P's metadata, and then return a pointer to P.
  pages_[free_frame_id].page_id_ = page_id;
  page_table_.insert({page_id, free_frame_id});
  pages_[free_frame_id].pin_count_ = 1;
  replacer_->Pin(free_frame_id);

  latch_.unlock();
  return &pages_[free_frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  // TODO: think about whether we should add lock
  latch_.lock();  // TODO: using 1 lock to lock everything could be blocking?
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  Page *pageToUnpin = &pages_[page_table_[page_id]];
  if (pageToUnpin->pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  pageToUnpin->pin_count_ -= 1;
  pageToUnpin->is_dirty_ = is_dirty;
  if (pageToUnpin->pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  Page *pageToFlush = &pages_[page_table_[page_id]];

  if (pageToFlush->is_dirty_) {
    disk_manager_->WritePage(pageToFlush->page_id_, pageToFlush->GetData());
    pageToFlush->is_dirty_ = false;
  }

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  latch_.lock();
  frame_id_t free_frame_id = -1;
  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&free_frame_id)) {
      latch_.unlock();
      return nullptr;
    }

    Page *replacedPage = &pages_[free_frame_id];
    if (replacedPage->is_dirty_) {
      FlushPageImpl(replacedPage->page_id_);
      replacedPage->is_dirty_ = false;
    }

    page_table_.erase(replacedPage->page_id_);
  }

  *page_id = disk_manager_->AllocatePage();
  pages_[free_frame_id].page_id_ = *page_id;
  page_table_.insert({*page_id, free_frame_id});
  pages_[free_frame_id].ResetMemory();
  replacer_->Pin(free_frame_id);
  pages_[free_frame_id].pin_count_ = 1;
  pages_[free_frame_id].is_dirty_ = false;  // TODO: for a newly created page, what should we set the dirty bit to?

  latch_.unlock();
  return &pages_[free_frame_id];
}

// NOTE: Assumes user calls flush to propagate any unsaved changes. This function just destroys and removes a page.
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // TODO: think about whether we should add lock
  if (page_table_.find(page_id) == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *pageToDelete = &pages_[frame_id];

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (pageToDelete->GetPinCount() > 0) {
    return false;
  }

  // 3.   Otherwise, P can be deleted.
  // Remove P from the page table, reset its metadata and return it to the free list.
  free_list_.emplace_back(page_table_[page_id]);
  page_table_.erase(pageToDelete->page_id_);
  pageToDelete->ResetMemory();
  // TODO refactor these lines to a function ResetPage()
  pageToDelete->page_id_ = INVALID_PAGE_ID;
  pageToDelete->is_dirty_ = false;
  pageToDelete->pin_count_ = 0;
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].page_id_);
  }
}

}  // namespace bustub