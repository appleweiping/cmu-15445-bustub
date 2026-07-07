#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  current_reads_[read_ts]++;
  ordered_reads_[read_ts]++;
  // The watermark is the smallest live read timestamp.
  watermark_ = ordered_reads_.begin()->first;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // Decrement the refcount for this read ts, dropping the entry at zero.
  auto it = ordered_reads_.find(read_ts);
  if (it != ordered_reads_.end()) {
    it->second--;
    if (it->second == 0) {
      ordered_reads_.erase(it);
    }
  }
  auto uit = current_reads_.find(read_ts);
  if (uit != current_reads_.end()) {
    uit->second--;
    if (uit->second == 0) {
      current_reads_.erase(uit);
    }
  }

  if (ordered_reads_.empty()) {
    watermark_ = commit_ts_;
  } else {
    watermark_ = ordered_reads_.begin()->first;
  }
}

}  // namespace bustub
