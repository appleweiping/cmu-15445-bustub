//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  // Single-pass build of the aggregate hash table.
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    AggregateKey key = MakeAggregateKey(&child_tuple);
    AggregateValue value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
  empty_emitted_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    const AggregateKey &key = aht_iterator_.Key();
    const AggregateValue &val = aht_iterator_.Val();
    // Output columns are group-by keys followed by aggregate results.
    std::vector<Value> values;
    values.reserve(key.group_bys_.size() + val.aggregates_.size());
    for (const auto &g : key.group_bys_) {
      values.push_back(g);
    }
    for (const auto &a : val.aggregates_) {
      values.push_back(a);
    }
    *tuple = Tuple(values, &GetOutputSchema());
    ++aht_iterator_;
    return true;
  }

  // Special case: an empty input with no GROUP BY still yields one row of
  // default aggregate values (e.g. count(*) = 0).
  if (!empty_emitted_ && aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    empty_emitted_ = true;
    AggregateValue default_val = aht_.GenerateInitialAggregateValue();
    *tuple = Tuple(default_val.aggregates_, &GetOutputSchema());
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
