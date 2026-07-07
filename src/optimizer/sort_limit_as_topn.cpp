#include <memory>
#include <vector>
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Optimize children bottom-up first.
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));

  // Collapse a Limit directly on top of a Sort into a single TopN.
  if (optimized->GetType() == PlanType::Limit) {
    const auto &limit = dynamic_cast<const LimitPlanNode &>(*optimized);
    const auto &child = optimized->GetChildAt(0);
    if (child->GetType() == PlanType::Sort) {
      const auto &sort = dynamic_cast<const SortPlanNode &>(*child);
      return std::make_shared<TopNPlanNode>(optimized->output_schema_, sort.GetChildAt(0), sort.GetOrderBy(),
                                            limit.GetLimit());
    }
  }
  return optimized;
}

}  // namespace bustub
