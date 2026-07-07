#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Optimize children bottom-up first.
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));

  if (optimized->GetType() != PlanType::SeqScan) {
    return optimized;
  }
  const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized);
  const auto &predicate = seq_scan.filter_predicate_;
  if (predicate == nullptr) {
    return optimized;
  }

  // Only a single `#col = const` (or `const = #col`) equality can use a point
  // lookup on the hash index (Fall 2023).
  const auto *cmp = dynamic_cast<const ComparisonExpression *>(predicate.get());
  if (cmp == nullptr || cmp->comp_type_ != ComparisonType::Equal) {
    return optimized;
  }

  const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(cmp->GetChildAt(0).get());
  const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(cmp->GetChildAt(1).get());
  if (col_expr == nullptr || const_expr == nullptr) {
    // Try the reversed operand order.
    col_expr = dynamic_cast<const ColumnValueExpression *>(cmp->GetChildAt(1).get());
    const_expr = dynamic_cast<const ConstantValueExpression *>(cmp->GetChildAt(0).get());
  }
  if (col_expr == nullptr || const_expr == nullptr) {
    return optimized;
  }

  // Is there a single-column index on this column?
  auto matched = MatchIndex(seq_scan.table_name_, col_expr->GetColIdx());
  if (!matched.has_value()) {
    return optimized;
  }
  auto [index_oid, index_name] = *matched;

  // The pred_key raw pointer aliases the constant owned by `filter_predicate_`,
  // which we pass along so it stays alive for the executor.
  auto *pred_key = const_cast<ConstantValueExpression *>(const_expr);
  return std::make_shared<IndexScanPlanNode>(seq_scan.output_schema_, seq_scan.GetTableOid(), index_oid,
                                            seq_scan.filter_predicate_, pred_key);
}

}  // namespace bustub
