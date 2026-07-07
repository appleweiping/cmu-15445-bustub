//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

auto NestedLoopJoinExecutor::MakeJoinTuple(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
  std::vector<Value> values;
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();
  values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    values.push_back(right_tuple->GetValue(&right_schema, i));
  }
  return Tuple(values, &GetOutputSchema());
}

auto NestedLoopJoinExecutor::MakeLeftJoinTuple(const Tuple *left_tuple) -> Tuple {
  std::vector<Value> values;
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();
  values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
  }
  return Tuple(values, &GetOutputSchema());
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  // Materialise the right side once so we can re-scan it per left tuple.
  right_tuples_.clear();
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    right_tuples_.push_back(right_tuple);
  }

  // Prime the first left tuple.
  RID left_rid;
  left_valid_ = left_executor_->Next(&left_tuple_, &left_rid);
  right_idx_ = 0;
  left_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &right_schema = right_executor_->GetOutputSchema();
  while (left_valid_) {
    // Scan remaining right tuples for the current left tuple.
    while (right_idx_ < right_tuples_.size()) {
      const Tuple &right_tuple = right_tuples_[right_idx_];
      right_idx_++;
      auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                    right_schema);
      if (!value.IsNull() && value.GetAs<bool>()) {
        left_matched_ = true;
        *tuple = MakeJoinTuple(&left_tuple_, &right_tuple);
        return true;
      }
    }

    // Right side exhausted for this left tuple. For a left join with no match,
    // emit the left tuple padded with NULLs.
    bool emit_left_null = (plan_->GetJoinType() == JoinType::LEFT) && !left_matched_;
    Tuple left_null_tuple;
    if (emit_left_null) {
      left_null_tuple = MakeLeftJoinTuple(&left_tuple_);
    }

    // Advance to the next left tuple.
    RID left_rid;
    left_valid_ = left_executor_->Next(&left_tuple_, &left_rid);
    right_idx_ = 0;
    left_matched_ = false;

    if (emit_left_null) {
      *tuple = left_null_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
