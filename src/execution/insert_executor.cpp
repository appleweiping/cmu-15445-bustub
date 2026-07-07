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
#include <vector>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
  auto *txn = exec_ctx_->GetTransaction();

  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  const auto &schema = table_info->schema_;
  uint32_t col_count = schema.GetColumnCount();

  int inserted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // Primary-key handling: check whether the key already exists in an index.
    bool reused = false;
    for (auto *index_info : indexes) {
      auto key = child_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      std::vector<RID> existing;
      index_info->index_->ScanKey(key, &existing, txn);
      if (existing.empty()) {
        continue;
      }
      RID existing_rid = existing[0];
      auto meta = table_info->table_->GetTupleMeta(existing_rid);

      if (!meta.is_deleted_) {
        // Key maps to a live tuple -> duplicate primary key -> conflict.
        txn->SetTainted();
        throw ExecutionException("write-write conflict: duplicate primary key on insert");
      }
      // Deleted tuple: reuse its RID unless another txn holds it.
      if (IsWriteWriteConflict(meta, txn->GetReadTs(), txn->GetTransactionTempTs())) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict: primary key held by another txn");
      }

      // Re-insert onto the deleted slot: emit an undo log for the deleted state,
      // then write the new tuple in place with our temporary timestamp.
      if (meta.ts_ != txn->GetTransactionTempTs()) {
        std::vector<bool> modified(col_count, true);
        UndoLog undo_log{true, modified, Tuple{}, meta.ts_,
                         txn_mgr->GetUndoLink(existing_rid).value_or(UndoLink{})};
        UndoLink new_link = txn->AppendUndoLog(undo_log);
        txn_mgr->UpdateUndoLink(existing_rid, new_link);
        txn->AppendWriteSet(plan_->GetTableOid(), existing_rid);
      }
      table_info->table_->UpdateTupleInPlace(TupleMeta{txn->GetTransactionTempTs(), false}, child_tuple,
                                             existing_rid);
      reused = true;
      break;
    }
    if (reused) {
      inserted++;
      continue;
    }

    // No existing key: insert a fresh tuple with the temporary timestamp.
    auto new_rid = table_info->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, child_tuple,
                                                   exec_ctx_->GetLockManager(), txn, plan_->GetTableOid());
    if (!new_rid.has_value()) {
      continue;
    }
    txn->AppendWriteSet(plan_->GetTableOid(), *new_rid);

    // Mirror the insert into every index. A failed InsertEntry means a concurrent
    // inserter won the race -> write-write conflict.
    for (auto *index_info : indexes) {
      auto key = child_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      if (!index_info->index_->InsertEntry(key, *new_rid, txn)) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict: concurrent primary key insert");
      }
    }
    inserted++;
  }

  // Emit a single one-column tuple carrying the number of inserted rows.
  std::vector<Value> values{Value(TypeId::INTEGER, inserted)};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
