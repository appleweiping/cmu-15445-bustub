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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  const auto &schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();

  int updated = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto [meta, old_tuple] = table_info_->table_->GetTuple(child_rid);

    // Write-write conflict detection.
    if (IsWriteWriteConflict(meta, txn->GetReadTs(), txn->GetTransactionTempTs())) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict on update");
    }

    // Compute the new tuple in place (Fall 2023 non-primary-key updates keep RID).
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&old_tuple, schema));
    }
    Tuple new_tuple(new_values, &schema);

    // Determine which columns actually changed (for the diff undo log).
    std::vector<bool> modified(col_count, false);
    std::vector<uint32_t> modified_cols;
    for (uint32_t i = 0; i < col_count; i++) {
      Value ov = old_tuple.GetValue(&schema, i);
      Value nv = new_tuple.GetValue(&schema, i);
      if (ov.CompareExactlyEquals(nv)) {
        continue;
      }
      modified[i] = true;
      modified_cols.push_back(i);
    }

    if (meta.ts_ != txn->GetTransactionTempTs()) {
      // First write by this txn: create a diff undo log holding the OLD values of
      // the changed columns, then link it into the version chain.
      Schema partial_schema = Schema::CopySchema(&schema, modified_cols);
      std::vector<Value> old_partial;
      old_partial.reserve(modified_cols.size());
      for (auto c : modified_cols) {
        old_partial.push_back(old_tuple.GetValue(&schema, c));
      }
      Tuple partial_tuple(old_partial, &partial_schema);
      UndoLog undo_log{false, modified, partial_tuple, meta.ts_,
                       txn_mgr->GetUndoLink(child_rid).value_or(UndoLink{})};
      UndoLink new_link = txn->AppendUndoLog(undo_log);
      txn_mgr->UpdateUndoLink(child_rid, new_link);
      txn->AppendWriteSet(plan_->GetTableOid(), child_rid);
    } else {
      // Self-update: merge new modifications into the existing head undo log so a
      // single log per RID captures the full pre-txn version.
      auto head_link_opt = txn_mgr->GetUndoLink(child_rid);
      if (head_link_opt.has_value() && head_link_opt->IsValid() &&
          head_link_opt->prev_txn_ == txn->GetTransactionId()) {
        UndoLog head = txn->GetUndoLog(head_link_opt->prev_log_idx_);
        // Reconstruct the full set of "old" values the head currently records.
        std::vector<uint32_t> head_cols;
        for (uint32_t i = 0; i < head.modified_fields_.size(); i++) {
          if (head.modified_fields_[i]) {
            head_cols.push_back(i);
          }
        }
        Schema head_schema = Schema::CopySchema(&schema, head_cols);
        std::vector<Value> merged_old(col_count);
        std::vector<bool> merged_fields(col_count, false);
        uint32_t hi = 0;
        for (uint32_t i = 0; i < col_count; i++) {
          if (head.modified_fields_[i]) {
            merged_old[i] = head.tuple_.GetValue(&head_schema, hi++);
            merged_fields[i] = true;
          }
        }
        // Add columns changed for the first time in this update (capture the
        // value they had *before* this update, i.e. old_tuple's value).
        for (auto c : modified_cols) {
          if (!merged_fields[c]) {
            merged_old[c] = old_tuple.GetValue(&schema, c);
            merged_fields[c] = true;
          }
        }
        std::vector<uint32_t> merged_cols;
        std::vector<Value> merged_partial;
        for (uint32_t i = 0; i < col_count; i++) {
          if (merged_fields[i]) {
            merged_cols.push_back(i);
            merged_partial.push_back(merged_old[i]);
          }
        }
        Schema merged_schema = Schema::CopySchema(&schema, merged_cols);
        Tuple merged_tuple(merged_partial, &merged_schema);
        UndoLog new_head{head.is_deleted_, merged_fields, merged_tuple, head.ts_, head.prev_version_};
        txn->ModifyUndoLog(head_link_opt->prev_log_idx_, new_head);
      }
    }

    // Apply the update in place with our temporary timestamp.
    table_info_->table_->UpdateTupleInPlace(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple, child_rid);
    updated++;
  }

  std::vector<Value> values{Value(TypeId::INTEGER, updated)};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
