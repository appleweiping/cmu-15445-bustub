#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // Start from the base tuple's column values (unless the base is a delete
  // marker with no prior undo logs, in which case the tuple does not exist).
  uint32_t col_count = schema->GetColumnCount();
  std::vector<Value> values;
  values.reserve(col_count);
  bool is_deleted = base_meta.is_deleted_;
  for (uint32_t i = 0; i < col_count; i++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }

  // Apply each undo log in order, walking back in time. Each log either deletes
  // the tuple or overwrites the columns flagged in modified_fields_.
  for (const auto &log : undo_logs) {
    if (log.is_deleted_) {
      is_deleted = true;
      // A delete marker resets all columns; subsequent logs may repopulate them.
      for (uint32_t i = 0; i < col_count; i++) {
        values[i] = ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType());
      }
      continue;
    }
    is_deleted = false;

    // Build the schema of the partial tuple (only the modified columns).
    std::vector<uint32_t> modified_cols;
    for (uint32_t i = 0; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        modified_cols.push_back(i);
      }
    }
    Schema partial_schema = Schema::CopySchema(schema, modified_cols);

    // Copy each modified column value out of the partial tuple.
    uint32_t partial_idx = 0;
    for (uint32_t i = 0; i < col_count; i++) {
      if (i < log.modified_fields_.size() && log.modified_fields_[i]) {
        values[i] = log.tuple_.GetValue(&partial_schema, partial_idx);
        partial_idx++;
      }
    }
  }

  if (is_deleted) {
    return std::nullopt;
  }
  return std::make_optional<Tuple>(values, schema);
}

auto CollectVisibleUndoLogs(TransactionManager *txn_mgr, RID rid, const TupleMeta &base_meta, timestamp_t read_ts,
                            txn_id_t self_txn_id, std::vector<UndoLog> *out_logs) -> bool {
  out_logs->clear();

  // The base tuple in the heap is directly visible when it was committed at or
  // before our read timestamp, or when we wrote it ourselves in this txn.
  if (base_meta.ts_ == self_txn_id || base_meta.ts_ <= read_ts) {
    return true;
  }

  // Otherwise walk the undo-log version chain, applying logs newer than read_ts
  // until we reach a version committed at or before read_ts.
  auto undo_link_opt = txn_mgr->GetUndoLink(rid);
  while (undo_link_opt.has_value() && undo_link_opt->IsValid()) {
    auto undo_log_opt = txn_mgr->GetUndoLogOptional(*undo_link_opt);
    if (!undo_log_opt.has_value()) {
      break;
    }
    out_logs->push_back(*undo_log_opt);
    if (undo_log_opt->ts_ <= read_ts) {
      // This version is the one visible at read_ts.
      return true;
    }
    undo_link_opt = undo_log_opt->prev_version_;
  }

  // No committed version is visible at read_ts.
  return false;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
