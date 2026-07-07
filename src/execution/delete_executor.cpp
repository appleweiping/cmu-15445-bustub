//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  done_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  done_ = true;

  auto *catalog = exec_ctx_->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  const auto &schema = table_info->schema_;

  int deleted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto [meta, base_tuple] = table_info->table_->GetTuple(child_rid);

    // Write-write conflict: another (uncommitted or newer-committed) txn owns
    // this tuple. Taint ourselves and abort.
    if (IsWriteWriteConflict(meta, txn->GetReadTs(), txn->GetTransactionTempTs())) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict on delete");
    }

    if (meta.ts_ != txn->GetTransactionTempTs()) {
      // First modification by this txn: push an undo log capturing the full old
      // tuple (all columns), then link it into the version chain.
      std::vector<bool> modified(schema.GetColumnCount(), true);
      UndoLog undo_log{false, modified, base_tuple, meta.ts_, txn_mgr->GetUndoLink(child_rid).value_or(UndoLink{})};
      UndoLink new_link = txn->AppendUndoLog(undo_log);
      txn_mgr->UpdateUndoLink(child_rid, new_link);
      txn->AppendWriteSet(plan_->GetTableOid(), child_rid);
    }
    // If we already own the tuple, we just flip it to a delete marker; any
    // existing undo log already captures the pre-txn version.

    // Mark the base tuple as deleted with our temporary timestamp.
    table_info->table_->UpdateTupleMeta(TupleMeta{txn->GetTransactionTempTs(), true}, child_rid);
    deleted++;
  }

  std::vector<Value> values{Value(TypeId::INTEGER, deleted)};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
