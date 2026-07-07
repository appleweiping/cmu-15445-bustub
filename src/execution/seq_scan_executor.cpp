//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // Snapshot all RIDs up front so iteration is stable across the scan.
  rids_.clear();
  for (auto it = table_info->table_->MakeIterator(); !it.IsEnd(); ++it) {
    rids_.push_back(it.GetRID());
  }
  rid_iter_ = rids_.begin();
  table_info_ = table_info;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &schema = table_info_->schema_;
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  while (rid_iter_ != rids_.end()) {
    RID cur_rid = *rid_iter_;
    ++rid_iter_;
    auto [meta, base_tuple] = table_info_->table_->GetTuple(cur_rid);

    Tuple visible_tuple = base_tuple;
    // Under MVCC (a real transaction/txn manager present), reconstruct the
    // version visible at this transaction's read timestamp.
    if (txn != nullptr && txn_mgr != nullptr) {
      std::vector<UndoLog> logs;
      if (!CollectVisibleUndoLogs(txn_mgr, cur_rid, meta, txn->GetReadTs(), txn->GetTransactionTempTs(), &logs)) {
        continue;  // no version visible to this reader
      }
      auto reconstructed = ReconstructTuple(&schema, base_tuple, meta, logs);
      if (!reconstructed.has_value()) {
        continue;  // tuple is deleted in the visible version
      }
      visible_tuple = *reconstructed;
    } else if (meta.is_deleted_) {
      continue;  // no MVCC context: honour the storage-level delete flag
    }

    // Apply the pushed-down filter predicate, if any.
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&visible_tuple, schema);
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }
    *tuple = visible_tuple;
    *rid = cur_rid;
    return true;
  }
  return false;
}

}  // namespace bustub
