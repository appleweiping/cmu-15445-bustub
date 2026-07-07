#include "execution/executors/sort_executor.h"
#include <algorithm>

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    sorted_tuples_.push_back(child_tuple);
  }
  TupleComparator cmp{&plan_->GetOrderBy(), &child_executor_->GetOutputSchema()};
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), cmp);
  cursor_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= sorted_tuples_.size()) {
    return false;
  }
  *tuple = sorted_tuples_[cursor_];
  *rid = tuple->GetRid();
  cursor_++;
  return true;
}

}  // namespace bustub
