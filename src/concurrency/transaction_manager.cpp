//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // A new transaction reads the latest committed snapshot.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // The commit timestamp is one past the last committed transaction.
  timestamp_t commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // Stamp every tuple this txn wrote with the final commit timestamp. During the
  // txn these tuples carried the transaction's temporary timestamp (its txn id).
  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto *table_info = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      auto meta = table_info->table_->GetTupleMeta(rid);
      meta.ts_ = commit_ts;
      table_info->table_->UpdateTupleMeta(meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  txn->commit_ts_ = commit_ts;
  txn->state_ = TransactionState::COMMITTED;
  last_commit_ts_.store(commit_ts);
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  timestamp_t watermark = GetWatermark();

  // A transaction survives GC if at least one of its undo logs is still needed
  // to reconstruct a version visible to some reader at read_ts >= watermark.
  std::unordered_set<txn_id_t> accessible;

  auto table_names = catalog_->GetTableNames();
  for (const auto &table_name : table_names) {
    auto *table_info = catalog_->GetTable(table_name);
    for (auto it = table_info->table_->MakeIterator(); !it.IsEnd(); ++it) {
      RID rid = it.GetRID();
      auto [meta, tuple] = it.GetTuple();

      // Walk the version chain. Once we pass a version committed at or before the
      // watermark, no reader needs any older undo log.
      bool reached_watermark = meta.ts_ <= watermark;
      auto link_opt = GetUndoLink(rid);
      while (link_opt.has_value() && link_opt->IsValid()) {
        if (reached_watermark) {
          break;  // this and all older logs are unreachable
        }
        auto log_opt = GetUndoLogOptional(*link_opt);
        if (!log_opt.has_value()) {
          break;  // owner txn already gone
        }
        accessible.insert(link_opt->prev_txn_);
        if (log_opt->ts_ <= watermark) {
          reached_watermark = true;
        }
        link_opt = log_opt->prev_version_;
      }
    }
  }

  // Remove committed/aborted transactions with no accessible undo logs.
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    const auto &txn = it->second;
    auto state = txn->GetTransactionState();
    bool finished = state == TransactionState::COMMITTED || state == TransactionState::ABORTED;
    if (finished && accessible.find(it->first) == accessible.end()) {
      it = txn_map_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace bustub
