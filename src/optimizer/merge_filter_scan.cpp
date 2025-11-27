//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// merge_filter_scan.cpp
//
// Identification: src/optimizer/merge_filter_scan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief merge filter into filter_predicate of seq scan plan node
 */
auto Optimizer::OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // std::cout << "走了mergefilter优化\n";
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }  // dfs,深度递归子节点

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      if (seq_scan_plan.filter_predicate_ != nullptr) return optimized_plan;
      auto new_seq_scan = std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                            seq_scan_plan.table_name_, filter_plan.GetPredicate());
      // 再尝试将SeqScan优化为IndexScan
      auto index_scan_plan = OptimizeSeqScanAsIndexScan(plan);
      // 判断返回的是否是IndexScan，若是则返回，否则返回下推后的SeqScan
      if (index_scan_plan->GetType() == PlanType::IndexScan) {
        return index_scan_plan;
      }
      std::cout << "没有对应索引\n";
      return new_seq_scan;
    }
  }

  return optimized_plan;
}

}  // namespace bustub
