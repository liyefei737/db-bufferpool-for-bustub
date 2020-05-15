//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  using std::vector;
  ref_bits_ = vector<bool>(num_pages, false);
  in_replacer_ = vector<bool>(num_pages, false);
  buffer_pool_size_ = num_pages;
  clockhand_ = 0;
  size_ = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if (size_ <= 0) {
    return false;
  }

  while (true) {
    if (!in_replacer_[clockhand_]) {
      clockhand_ = (clockhand_ + 1) % buffer_pool_size_;
      continue;
    }
    if (ref_bits_[clockhand_]) {
      ref_bits_[clockhand_] = false;
      clockhand_ = (clockhand_ + 1) % buffer_pool_size_;
      continue;
    }

    // frame in replacer and reft bit is 0
    *frame_id = clockhand_;
    in_replacer_[clockhand_] = false;
    ref_bits_[clockhand_] = false;
    size_ -= 1;
    break;
  }
  latch_.unlock();
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  if (in_replacer_[frame_id]) {
    in_replacer_[frame_id] = false;
    ref_bits_[frame_id] = false;
    size_ -= 1;
  }

  latch_.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  if (!in_replacer_[frame_id]) {
    in_replacer_[frame_id] = true;
    size_++;
  }
  latch_.unlock();
}

size_t ClockReplacer::Size() { return size_; }

}  // namespace bustub
