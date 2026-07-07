//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  auto *catalog = exec_ctx_->GetCatalog();
  inner_table_info_ = catalog->GetTable(plan_->GetInnerTableOid());
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &outer_schema = child_executor_->GetOutputSchema();
  const auto &inner_schema = plan_->InnerTableSchema();

  Tuple outer_tuple;
  RID outer_rid;
  while (child_executor_->Next(&outer_tuple, &outer_rid)) {
    // Build the index probe key from the outer tuple's key predicate value.
    Value key_value = plan_->KeyPredicate()->Evaluate(&outer_tuple, outer_schema);
    std::vector<Value> key_values{key_value};
    Tuple probe_key(key_values, &index_info_->key_schema_);

    std::vector<RID> matches;
    index_info_->index_->ScanKey(probe_key, &matches, exec_ctx_->GetTransaction());

    if (!matches.empty()) {
      // Fall 2023 inner index is a unique key; take the first live match.
      auto [meta, inner_tuple] = inner_table_info_->table_->GetTuple(matches[0]);
      if (!meta.is_deleted_) {
        std::vector<Value> values;
        values.reserve(outer_schema.GetColumnCount() + inner_schema.GetColumnCount());
        for (uint32_t i = 0; i < outer_schema.GetColumnCount(); i++) {
          values.push_back(outer_tuple.GetValue(&outer_schema, i));
        }
        for (uint32_t i = 0; i < inner_schema.GetColumnCount(); i++) {
          values.push_back(inner_tuple.GetValue(&inner_schema, i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      }
    }

    // No inner match: for a left join, emit the outer tuple padded with NULLs.
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      values.reserve(outer_schema.GetColumnCount() + inner_schema.GetColumnCount());
      for (uint32_t i = 0; i < outer_schema.GetColumnCount(); i++) {
        values.push_back(outer_tuple.GetValue(&outer_schema, i));
      }
      for (uint32_t i = 0; i < inner_schema.GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(inner_schema.GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    // Inner join with no match: skip this outer tuple.
  }
  return false;
}

}  // namespace bustub
