#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  TupleComparator cmp{&plan_->GetOrderBy(), &child_executor_->GetOutputSchema()};
  // A bounded max-heap under `cmp`: the heap top is the "largest" of the kept
  // tuples, so once it exceeds N we drop the top. This keeps the N smallest.
  auto heap_cmp = [&cmp](const Tuple &a, const Tuple &b) { return cmp(a, b); };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(heap_cmp)> heap(heap_cmp);

  size_t n = plan_->GetN();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    heap.push(child_tuple);
    if (heap.size() > n) {
      heap.pop();
    }
  }

  // Drain heap (largest first) then reverse to get ascending output order.
  top_entries_.clear();
  top_entries_.reserve(heap.size());
  while (!heap.empty()) {
    top_entries_.push_back(heap.top());
    heap.pop();
  }
  std::reverse(top_entries_.begin(), top_entries_.end());
  cursor_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= top_entries_.size()) {
    return false;
  }
  *tuple = top_entries_[cursor_];
  *rid = tuple->GetRid();
  cursor_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size() - cursor_; }

}  // namespace bustub
