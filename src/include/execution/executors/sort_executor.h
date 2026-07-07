//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  /** Child executor supplying the tuples to sort. */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Fully sorted tuples produced at Init time. */
  std::vector<Tuple> sorted_tuples_;
  /** Cursor into `sorted_tuples_`. */
  size_t cursor_{0};
};

/**
 * Strict-weak ordering over tuples for a list of (OrderByType, expr) keys.
 * Returns true if `a` should come before `b`. Shared by Sort and TopN.
 */
struct TupleComparator {
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order_bys_;
  const Schema *schema_;

  auto operator()(const Tuple &a, const Tuple &b) const -> bool {
    for (const auto &[type, expr] : *order_bys_) {
      Value va = expr->Evaluate(&a, *schema_);
      Value vb = expr->Evaluate(&b, *schema_);
      if (va.CompareEquals(vb) == CmpBool::CmpTrue) {
        continue;
      }
      bool less = va.CompareLessThan(vb) == CmpBool::CmpTrue;
      if (type == OrderByType::DESC) {
        return !less;
      }
      // DEFAULT / ASC / INVALID all treated as ascending.
      return less;
    }
    return false;
  }
};
}  // namespace bustub
