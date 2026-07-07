#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

class TransactionManager;

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

/**
 * @brief Walk the version chain for `rid` and collect the undo logs needed to
 * reconstruct the version visible to a reader at `read_ts` (a tuple written by
 * the reader's own transaction, identified by `self_txn_id`, is also visible).
 *
 * @return true if a visible version exists (out_logs holds the logs to apply to
 * the base tuple, possibly empty when the base itself is visible); false if no
 * committed version is visible at read_ts.
 */
auto CollectVisibleUndoLogs(TransactionManager *txn_mgr, RID rid, const TupleMeta &base_meta, timestamp_t read_ts,
                            txn_id_t self_txn_id, std::vector<UndoLog> *out_logs) -> bool;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
