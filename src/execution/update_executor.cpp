//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  done_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  done_ = true;

  auto *catalog = exec_ctx_->GetCatalog();
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  const auto &schema = table_info_->schema_;

  int updated = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // Model an update as delete-old + insert-new (the P3 storage model).
    table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, child_rid);
    for (auto *index_info : indexes) {
      auto old_key =
          child_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
    }

    // Compute the new tuple from the target expressions.
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tuple, schema));
    }
    Tuple new_tuple(new_values, &schema);

    auto new_rid = table_info_->table_->InsertTuple(TupleMeta{0, false}, new_tuple, exec_ctx_->GetLockManager(),
                                                    exec_ctx_->GetTransaction(), plan_->GetTableOid());
    if (new_rid.has_value()) {
      for (auto *index_info : indexes) {
        auto new_key =
            new_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_key, *new_rid, exec_ctx_->GetTransaction());
      }
    }
    updated++;
  }

  std::vector<Value> values{Value(TypeId::INTEGER, updated)};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
