//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/index/extendible_hash_table_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  table_info_ = catalog->GetTable(index_info_->table_name_);

  // Point-lookup on the index using the constant predicate key.
  result_rids_.clear();
  if (plan_->pred_key_ != nullptr) {
    Value key_value = plan_->pred_key_->val_;
    std::vector<Value> key_values{key_value};
    Tuple key(key_values, &index_info_->key_schema_);
    index_info_->index_->ScanKey(key, &result_rids_, exec_ctx_->GetTransaction());
  }
  rid_iter_ = result_rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &schema = table_info_->schema_;
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  while (rid_iter_ != result_rids_.end()) {
    RID cur_rid = *rid_iter_;
    ++rid_iter_;
    auto [meta, base_tuple] = table_info_->table_->GetTuple(cur_rid);

    Tuple visible_tuple = base_tuple;
    // Under MVCC, reconstruct the version visible at this txn's read timestamp.
    if (txn != nullptr && txn_mgr != nullptr) {
      std::vector<UndoLog> logs;
      if (!CollectVisibleUndoLogs(txn_mgr, cur_rid, meta, txn->GetReadTs(), txn->GetTransactionTempTs(), &logs)) {
        continue;
      }
      auto reconstructed = ReconstructTuple(&schema, base_tuple, meta, logs);
      if (!reconstructed.has_value()) {
        continue;  // deleted in the visible version
      }
      visible_tuple = *reconstructed;
    } else if (meta.is_deleted_) {
      continue;
    }

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
