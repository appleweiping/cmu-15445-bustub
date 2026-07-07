#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include <unordered_map>
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

namespace {

/** Combine one input value into a running window aggregate of the given type. */
auto CombineWindow(WindowFunctionType type, const Value &acc, const Value &in) -> Value {
  switch (type) {
    case WindowFunctionType::CountStarAggregate:
      return acc.Add(ValueFactory::GetIntegerValue(1));
    case WindowFunctionType::CountAggregate:
      if (in.IsNull()) {
        return acc;
      }
      return acc.IsNull() ? ValueFactory::GetIntegerValue(1) : acc.Add(ValueFactory::GetIntegerValue(1));
    case WindowFunctionType::SumAggregate:
      if (in.IsNull()) {
        return acc;
      }
      return acc.IsNull() ? in : acc.Add(in);
    case WindowFunctionType::MinAggregate:
      if (in.IsNull()) {
        return acc;
      }
      return acc.IsNull() ? in : acc.Min(in);
    case WindowFunctionType::MaxAggregate:
      if (in.IsNull()) {
        return acc;
      }
      return acc.IsNull() ? in : acc.Max(in);
    case WindowFunctionType::Rank:
      return acc;  // handled separately
  }
  return acc;
}

auto InitialWindowValue(WindowFunctionType type) -> Value {
  if (type == WindowFunctionType::CountStarAggregate) {
    return ValueFactory::GetIntegerValue(0);
  }
  return ValueFactory::GetNullValueByType(TypeId::INTEGER);
}

}  // namespace

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  output_tuples_.clear();
  cursor_ = 0;

  const auto &child_schema = child_executor_->GetOutputSchema();

  // Materialise all child tuples.
  std::vector<Tuple> tuples;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples.push_back(child_tuple);
  }

  // All window functions in a plan share the same ORDER BY (Fall 2023). Pick the
  // first non-empty order_by to establish a global sort; also note whether any
  // window function specifies an ORDER BY (running vs whole-partition frames).
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> *global_order = nullptr;
  bool has_order = false;
  for (const auto &[idx, wf] : plan_->window_functions_) {
    if (!wf.order_by_.empty()) {
      global_order = &wf.order_by_;
      has_order = true;
      break;
    }
  }
  if (global_order != nullptr) {
    std::stable_sort(tuples.begin(), tuples.end(),
                     [&](const Tuple &a, const Tuple &b) {
                       for (const auto &[type, expr] : *global_order) {
                         Value va = expr->Evaluate(&a, child_schema);
                         Value vb = expr->Evaluate(&b, child_schema);
                         if (va.CompareEquals(vb) == CmpBool::CmpTrue) {
                           continue;
                         }
                         bool less = va.CompareLessThan(vb) == CmpBool::CmpTrue;
                         return type == OrderByType::DESC ? !less : less;
                       }
                       return false;
                     });
  }

  size_t n = tuples.size();
  // For each window-function output column index, its per-row result value.
  std::unordered_map<uint32_t, std::vector<Value>> results;

  auto partition_key = [&](const Tuple &t, const std::vector<AbstractExpressionRef> &pbys) -> std::vector<Value> {
    std::vector<Value> key;
    key.reserve(pbys.size());
    for (const auto &expr : pbys) {
      key.push_back(expr->Evaluate(&t, child_schema));
    }
    return key;
  };
  auto same_partition = [](const std::vector<Value> &a, const std::vector<Value> &b) -> bool {
    if (a.size() != b.size()) {
      return false;
    }
    for (size_t i = 0; i < a.size(); i++) {
      if (a[i].CompareEquals(b[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  };
  auto same_order_key = [&](const Tuple &a, const Tuple &b,
                            const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &obys) -> bool {
    for (const auto &[type, expr] : obys) {
      if (expr->Evaluate(&a, child_schema).CompareEquals(expr->Evaluate(&b, child_schema)) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  };

  for (const auto &[col_idx, wf] : plan_->window_functions_) {
    std::vector<Value> col_results(n);

    if (wf.type_ == WindowFunctionType::Rank) {
      // Rank within each partition using the window's order-by; ties share rank.
      size_t i = 0;
      while (i < n) {
        size_t j = i;
        std::vector<Value> pkey = partition_key(tuples[i], wf.partition_by_);
        uint32_t rank = 0;
        uint32_t seen = 0;
        while (j < n && same_partition(partition_key(tuples[j], wf.partition_by_), pkey)) {
          seen++;
          if (j == i || !same_order_key(tuples[j], tuples[j - 1], wf.order_by_)) {
            rank = seen;
          }
          col_results[j] = ValueFactory::GetIntegerValue(static_cast<int>(rank));
          j++;
        }
        i = j;
      }
    } else if (has_order) {
      // Running aggregate: UNBOUNDED PRECEDING .. CURRENT ROW within a partition.
      size_t i = 0;
      while (i < n) {
        size_t j = i;
        std::vector<Value> pkey = partition_key(tuples[i], wf.partition_by_);
        Value acc = InitialWindowValue(wf.type_);
        while (j < n && same_partition(partition_key(tuples[j], wf.partition_by_), pkey)) {
          Value in = wf.function_->Evaluate(&tuples[j], child_schema);
          acc = CombineWindow(wf.type_, acc, in);
          col_results[j] = acc;
          j++;
        }
        i = j;
      }
    } else {
      // No ORDER BY: whole-partition aggregate assigned to every row.
      // First compute per-partition totals, then broadcast.
      size_t i = 0;
      while (i < n) {
        size_t j = i;
        std::vector<Value> pkey = partition_key(tuples[i], wf.partition_by_);
        Value acc = InitialWindowValue(wf.type_);
        while (j < n && same_partition(partition_key(tuples[j], wf.partition_by_), pkey)) {
          Value in = wf.function_->Evaluate(&tuples[j], child_schema);
          acc = CombineWindow(wf.type_, acc, in);
          j++;
        }
        for (size_t k = i; k < j; k++) {
          col_results[k] = acc;
        }
        i = j;
      }
    }
    results[col_idx] = std::move(col_results);
  }

  // Assemble output rows: plain columns come from `columns_`, window columns
  // come from the precomputed `results`.
  for (size_t r = 0; r < n; r++) {
    std::vector<Value> row;
    row.reserve(plan_->columns_.size());
    for (uint32_t c = 0; c < plan_->columns_.size(); c++) {
      auto wf_it = results.find(c);
      if (wf_it != results.end()) {
        row.push_back(wf_it->second[r]);
      } else {
        row.push_back(plan_->columns_[c]->Evaluate(&tuples[r], child_schema));
      }
    }
    output_tuples_.emplace_back(row, &GetOutputSchema());
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= output_tuples_.size()) {
    return false;
  }
  *tuple = output_tuples_[cursor_];
  *rid = RID{};
  cursor_++;
  return true;
}
}  // namespace bustub
