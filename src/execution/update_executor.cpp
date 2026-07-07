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
#include <utility>
#include <vector>

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

auto UpdateExecutor::ModifiesPrimaryKey(const std::vector<uint32_t> &modified_cols) -> bool {
  // Any index whose key column is modified requires delete+insert so the index
  // stays consistent (primary-key indexes especially, but also secondary ones).
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto *index_info : indexes) {
    for (auto key_col : index_info->index_->GetKeyAttrs()) {
      for (auto mc : modified_cols) {
        if (key_col == mc) {
          return true;
        }
      }
    }
  }
  return false;
}

void UpdateExecutor::UpdateTupleInPlaceMVCC(RID rid, const TupleMeta &meta, const Tuple &old_tuple,
                                            const Tuple &new_tuple) {
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  const auto &schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();

  std::vector<bool> modified(col_count, false);
  std::vector<uint32_t> modified_cols;
  for (uint32_t i = 0; i < col_count; i++) {
    if (!old_tuple.GetValue(&schema, i).CompareExactlyEquals(new_tuple.GetValue(&schema, i))) {
      modified[i] = true;
      modified_cols.push_back(i);
    }
  }

  if (meta.ts_ != txn->GetTransactionTempTs()) {
    // First write by this txn: diff undo log of the OLD values of changed columns.
    Schema partial_schema = Schema::CopySchema(&schema, modified_cols);
    std::vector<Value> old_partial;
    for (auto c : modified_cols) {
      old_partial.push_back(old_tuple.GetValue(&schema, c));
    }
    Tuple partial_tuple(old_partial, &partial_schema);
    auto prev_link = txn_mgr->GetUndoLink(rid).value_or(UndoLink{});
    UndoLog undo_log{false, modified, partial_tuple, meta.ts_, prev_link};
    UndoLink new_link = txn->AppendUndoLog(undo_log);
    // Atomically install our version only if no other writer beat us (CAS on the
    // version link). A failed CAS means a concurrent write-write conflict.
    bool ok = txn_mgr->UpdateUndoLink(rid, new_link, [prev_link](std::optional<UndoLink> cur) -> bool {
      return !cur.has_value() || *cur == prev_link;
    });
    if (!ok) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict (concurrent update)");
    }
    txn->AppendWriteSet(plan_->GetTableOid(), rid);
  } else {
    // Self-update: merge into the existing (non-delete) head undo log.
    auto head_link_opt = txn_mgr->GetUndoLink(rid);
    if (head_link_opt.has_value() && head_link_opt->IsValid() &&
        head_link_opt->prev_txn_ == txn->GetTransactionId() &&
        !txn->GetUndoLog(head_link_opt->prev_log_idx_).is_deleted_) {
      UndoLog head = txn->GetUndoLog(head_link_opt->prev_log_idx_);
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

  table_info_->table_->UpdateTupleInPlace(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple, rid);
}

void UpdateExecutor::DeleteTupleMVCC(RID rid, const TupleMeta &meta, const Tuple &old_tuple) {
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  const auto &schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();

  if (meta.ts_ != txn->GetTransactionTempTs()) {
    std::vector<bool> modified(col_count, true);
    auto prev_link = txn_mgr->GetUndoLink(rid).value_or(UndoLink{});
    UndoLog undo_log{false, modified, old_tuple, meta.ts_, prev_link};
    UndoLink new_link = txn->AppendUndoLog(undo_log);
    bool ok = txn_mgr->UpdateUndoLink(rid, new_link, [prev_link](std::optional<UndoLink> cur) -> bool {
      return !cur.has_value() || *cur == prev_link;
    });
    if (!ok) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict (concurrent delete)");
    }
    txn->AppendWriteSet(plan_->GetTableOid(), rid);
  }
  table_info_->table_->UpdateTupleMeta(TupleMeta{txn->GetTransactionTempTs(), true}, rid);

  // Remove secondary (non-primary) index entries for the old tuple; primary-key
  // index entries are retained so the RID can be reused on re-insert.
  Tuple mutable_old = old_tuple;
  for (auto *index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    if (index_info->is_primary_key_) {
      continue;
    }
    auto key = mutable_old.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->DeleteEntry(key, rid, txn);
  }
}

void UpdateExecutor::InsertTupleMVCC(const Tuple &new_tuple_in) {
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  const auto &schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple new_tuple = new_tuple_in;  // KeyFromTuple is non-const

  // Reuse a deleted RID only when a PRIMARY-key index already maps this key.
  for (auto *index_info : indexes) {
    if (!index_info->is_primary_key_) {
      continue;
    }
    auto key = new_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    std::vector<RID> existing;
    index_info->index_->ScanKey(key, &existing, txn);
    if (existing.empty()) {
      continue;
    }
    RID existing_rid = existing[0];
    auto meta = table_info_->table_->GetTupleMeta(existing_rid);
    if (!meta.is_deleted_) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict: duplicate primary key on update");
    }
    if (IsWriteWriteConflict(meta, txn->GetReadTs(), txn->GetTransactionTempTs())) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict: primary key held by another txn");
    }
    if (meta.ts_ != txn->GetTransactionTempTs()) {
      std::vector<bool> modified(col_count, true);
      UndoLog undo_log{true, modified, Tuple{}, meta.ts_, txn_mgr->GetUndoLink(existing_rid).value_or(UndoLink{})};
      UndoLink new_link = txn->AppendUndoLog(undo_log);
      txn_mgr->UpdateUndoLink(existing_rid, new_link);
      txn->AppendWriteSet(plan_->GetTableOid(), existing_rid);
    }
    table_info_->table_->UpdateTupleInPlace(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple, existing_rid);
    // Also (re)establish secondary index entries pointing at the reused RID.
    for (auto *sec : indexes) {
      if (sec->is_primary_key_) {
        continue;
      }
      auto sk = new_tuple.KeyFromTuple(schema, sec->key_schema_, sec->index_->GetKeyAttrs());
      sec->index_->InsertEntry(sk, existing_rid, txn);
    }
    return;
  }

  // Otherwise a fresh insert with new index entries for every index.
  auto new_rid = table_info_->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple,
                                                  exec_ctx_->GetLockManager(), txn, plan_->GetTableOid());
  if (!new_rid.has_value()) {
    return;
  }
  txn->AppendWriteSet(plan_->GetTableOid(), *new_rid);
  for (auto *index_info : indexes) {
    auto key = new_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    if (!index_info->index_->InsertEntry(key, *new_rid, txn) && index_info->is_primary_key_) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict: concurrent primary key insert on update");
    }
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  done_ = true;

  auto *txn = exec_ctx_->GetTransaction();
  const auto &schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();

  // Materialise all target rows first so a primary-key update can delete every
  // old row before inserting new ones (avoids spurious self-collisions).
  std::vector<std::pair<RID, Tuple>> targets;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    targets.emplace_back(child_rid, child_tuple);
  }

  // Decide up front whether this update touches a primary-key column.
  bool pk_update = false;
  for (const auto &[t_rid, t_tuple] : targets) {
    auto [meta, old_tuple] = table_info_->table_->GetTuple(t_rid);
    std::vector<Value> new_values;
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&old_tuple, schema));
    }
    Tuple new_tuple(new_values, &schema);
    std::vector<uint32_t> modified_cols;
    for (uint32_t i = 0; i < col_count; i++) {
      if (!old_tuple.GetValue(&schema, i).CompareExactlyEquals(new_tuple.GetValue(&schema, i))) {
        modified_cols.push_back(i);
      }
    }
    if (ModifiesPrimaryKey(modified_cols)) {
      pk_update = true;
      break;
    }
  }

  int updated = 0;
  if (pk_update) {
    // Phase 1: conflict-check and delete every old row.
    std::vector<Tuple> new_tuples;
    for (const auto &[t_rid, t_tuple] : targets) {
      auto [meta, old_tuple] = table_info_->table_->GetTuple(t_rid);
      if (IsWriteWriteConflict(meta, txn->GetReadTs(), txn->GetTransactionTempTs())) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict on primary-key update");
      }
      std::vector<Value> new_values;
      for (const auto &expr : plan_->target_expressions_) {
        new_values.push_back(expr->Evaluate(&old_tuple, schema));
      }
      new_tuples.emplace_back(new_values, &schema);
      DeleteTupleMVCC(t_rid, meta, old_tuple);
      updated++;
    }
    // Phase 2: insert the new rows (reusing deleted RIDs when the key matches).
    for (const auto &nt : new_tuples) {
      InsertTupleMVCC(nt);
    }
  } else {
    for (const auto &[t_rid, t_tuple] : targets) {
      auto [meta, old_tuple] = table_info_->table_->GetTuple(t_rid);
      if (IsWriteWriteConflict(meta, txn->GetReadTs(), txn->GetTransactionTempTs())) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict on update");
      }
      std::vector<Value> new_values;
      for (const auto &expr : plan_->target_expressions_) {
        new_values.push_back(expr->Evaluate(&old_tuple, schema));
      }
      Tuple new_tuple(new_values, &schema);
      UpdateTupleInPlaceMVCC(t_rid, meta, old_tuple, new_tuple);
      updated++;
    }
  }

  std::vector<Value> values{Value(TypeId::INTEGER, updated)};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
