#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

namespace {

/**
 * Recursively collect equi-join key pairs from a join predicate. The predicate
 * must be a conjunction (AND-tree) of `col = col` comparisons where one side
 * references the left child (tuple_idx 0) and the other the right (tuple_idx 1).
 * Returns false if the predicate is not a pure equi-join conjunction.
 */
auto CollectEquiKeys(const AbstractExpression *expr, std::vector<AbstractExpressionRef> *left_keys,
                     std::vector<AbstractExpressionRef> *right_keys) -> bool {
  if (const auto *logic = dynamic_cast<const LogicExpression *>(expr); logic != nullptr) {
    if (logic->logic_type_ != LogicType::And) {
      return false;
    }
    return CollectEquiKeys(logic->GetChildAt(0).get(), left_keys, right_keys) &&
           CollectEquiKeys(logic->GetChildAt(1).get(), left_keys, right_keys);
  }

  const auto *cmp = dynamic_cast<const ComparisonExpression *>(expr);
  if (cmp == nullptr || cmp->comp_type_ != ComparisonType::Equal) {
    return false;
  }
  const auto *lhs = dynamic_cast<const ColumnValueExpression *>(cmp->GetChildAt(0).get());
  const auto *rhs = dynamic_cast<const ColumnValueExpression *>(cmp->GetChildAt(1).get());
  if (lhs == nullptr || rhs == nullptr) {
    return false;
  }

  // Normalise so the left-child column feeds left_keys and the right-child
  // column feeds right_keys, regardless of which side of `=` it appeared on.
  if (lhs->GetTupleIdx() == 0 && rhs->GetTupleIdx() == 1) {
    left_keys->push_back(std::make_shared<ColumnValueExpression>(0, lhs->GetColIdx(), lhs->GetReturnType()));
    right_keys->push_back(std::make_shared<ColumnValueExpression>(0, rhs->GetColIdx(), rhs->GetReturnType()));
    return true;
  }
  if (lhs->GetTupleIdx() == 1 && rhs->GetTupleIdx() == 0) {
    left_keys->push_back(std::make_shared<ColumnValueExpression>(0, rhs->GetColIdx(), rhs->GetReturnType()));
    right_keys->push_back(std::make_shared<ColumnValueExpression>(0, lhs->GetColIdx(), lhs->GetReturnType()));
    return true;
  }
  return false;
}

}  // namespace

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Optimize children bottom-up first.
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));

  if (optimized->GetType() != PlanType::NestedLoopJoin) {
    return optimized;
  }
  const auto &nlj = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized);

  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;
  if (nlj.Predicate() == nullptr || !CollectEquiKeys(nlj.Predicate().get(), &left_keys, &right_keys) ||
      left_keys.empty()) {
    return optimized;  // not a pure equi-join; leave as nested-loop join
  }

  return std::make_shared<HashJoinPlanNode>(nlj.output_schema_, nlj.GetChildAt(0), nlj.GetChildAt(1),
                                            std::move(left_keys), std::move(right_keys), nlj.GetJoinType());
}

}  // namespace bustub
