//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

auto HashJoinExecutor::MakeLeftKey(const Tuple *tuple) -> HashJoinKey {
  std::vector<Value> keys;
  keys.reserve(plan_->LeftJoinKeyExpressions().size());
  for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
    keys.push_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
  }
  return {std::move(keys)};
}

auto HashJoinExecutor::MakeRightKey(const Tuple *tuple) -> HashJoinKey {
  std::vector<Value> keys;
  keys.reserve(plan_->RightJoinKeyExpressions().size());
  for (const auto &expr : plan_->RightJoinKeyExpressions()) {
    keys.push_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
  }
  return {std::move(keys)};
}

auto HashJoinExecutor::MakeOutputTuple(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
  const auto &left_schema = left_child_->GetOutputSchema();
  const auto &right_schema = right_child_->GetOutputSchema();
  std::vector<Value> values;
  values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    if (right_tuple != nullptr) {
      values.push_back(right_tuple->GetValue(&right_schema, i));
    } else {
      values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
    }
  }
  return Tuple(values, &GetOutputSchema());
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  // Build phase: hash all right tuples by their join key.
  right_ht_.clear();
  Tuple right_tuple;
  RID right_rid;
  while (right_child_->Next(&right_tuple, &right_rid)) {
    right_ht_[MakeRightKey(&right_tuple)].push_back(right_tuple);
  }

  // Prime the probe with the first left tuple.
  RID left_rid;
  left_valid_ = left_child_->Next(&left_tuple_, &left_rid);
  cur_matches_ = nullptr;
  match_idx_ = 0;
  left_matched_ = false;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_valid_) {
    // Emit remaining matches for the current left tuple.
    if (cur_matches_ != nullptr && match_idx_ < cur_matches_->size()) {
      const Tuple &right_tuple = (*cur_matches_)[match_idx_];
      match_idx_++;
      left_matched_ = true;
      *tuple = MakeOutputTuple(&left_tuple_, &right_tuple);
      return true;
    }

    // Current left tuple exhausted. Look up its key on first visit.
    if (cur_matches_ == nullptr) {
      auto it = right_ht_.find(MakeLeftKey(&left_tuple_));
      if (it != right_ht_.end()) {
        cur_matches_ = &it->second;
        match_idx_ = 0;
        continue;  // re-enter loop to emit matches
      }
      // No matches at all.
    }

    // For a left join with no match, emit the left tuple padded with NULLs.
    bool emit_left_null = (plan_->GetJoinType() == JoinType::LEFT) && !left_matched_;
    Tuple left_null_tuple;
    if (emit_left_null) {
      left_null_tuple = MakeOutputTuple(&left_tuple_, nullptr);
    }

    // Advance to the next left tuple.
    RID left_rid;
    left_valid_ = left_child_->Next(&left_tuple_, &left_rid);
    cur_matches_ = nullptr;
    match_idx_ = 0;
    left_matched_ = false;

    if (emit_left_null) {
      *tuple = left_null_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
