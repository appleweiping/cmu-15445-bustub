//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  done_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  done_ = true;

  auto *catalog = exec_ctx_->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  auto indexes = catalog->GetTableIndexes(table_info->name_);

  int inserted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // Insert into the table heap.
    auto new_rid = table_info->table_->InsertTuple(TupleMeta{0, false}, child_tuple, exec_ctx_->GetLockManager(),
                                                   exec_ctx_->GetTransaction(), plan_->GetTableOid());
    if (!new_rid.has_value()) {
      continue;
    }
    // Mirror the insert into every index on the table.
    for (auto *index_info : indexes) {
      auto key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                          index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, *new_rid, exec_ctx_->GetTransaction());
    }
    inserted++;
  }

  // Emit a single one-column tuple carrying the number of inserted rows.
  std::vector<Value> values{Value(TypeId::INTEGER, inserted)};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
