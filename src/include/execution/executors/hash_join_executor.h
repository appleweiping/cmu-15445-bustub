//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/** A hash-join key: the tuple of evaluated join-key values. */
struct HashJoinKey {
  std::vector<Value> keys_;

  auto operator==(const HashJoinKey &other) const -> bool {
    if (keys_.size() != other.keys_.size()) {
      return false;
    }
    for (size_t i = 0; i < keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {
/** Hash for HashJoinKey, combining the hashes of the non-null key values. */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> std::size_t {
    std::size_t h = 0;
    for (const auto &value : key.keys_) {
      if (!value.IsNull()) {
        h = bustub::HashUtil::CombineHashes(h, bustub::HashUtil::HashValue(&value));
      }
    }
    return h;
  }
};
}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** Evaluate the left/right join-key expressions against a tuple. */
  auto MakeLeftKey(const Tuple *tuple) -> HashJoinKey;
  auto MakeRightKey(const Tuple *tuple) -> HashJoinKey;
  /** Build a joined output tuple (right side NULLs when right_tuple == nullptr). */
  auto MakeOutputTuple(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple;

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  /** Build-side hash table: right-key -> matching right tuples. */
  std::unordered_map<HashJoinKey, std::vector<Tuple>> right_ht_;

  /** Probe state for the current left tuple. */
  Tuple left_tuple_;
  bool left_valid_{false};
  const std::vector<Tuple> *cur_matches_{nullptr};
  size_t match_idx_{0};
  bool left_matched_{false};
};

}  // namespace bustub
