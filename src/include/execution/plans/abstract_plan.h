//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// abstract_plan.h
//
// 标识：src/include/execution/plans/abstract_plan.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "fmt/format.h"

namespace bustub {

#define BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(cname)                                                          \
  auto CloneWithChildren(std::vector<AbstractPlanNodeRef> children) const->std::unique_ptr<AbstractPlanNode> \
      override {                                                                                             \
    auto plan_node = cname(*this);                                                                           \
    plan_node.children_ = children;                                                                          \
    return std::make_unique<cname>(std::move(plan_node));                                                    \
  }

/** PlanType 表示系统中所有可能的计划节点类型。 */
enum class PlanType {
  SeqScan,          // 顺序扫描
  IndexScan,        // 索引扫描
  Insert,           // 插入操作
  Update,           // 更新操作
  Delete,           // 删除操作
  Aggregation,      // 聚合操作
  Limit,            // 限制返回行数
  NestedLoopJoin,   // 嵌套循环连接
  NestedIndexJoin,  // 嵌套索引连接
  HashJoin,         // 哈希连接
  Filter,           // 过滤操作
  Values,           // 值集合
  Projection,       // 投影操作
  Sort,             // 排序操作
  TopN,             // 取前N条记录
  TopNPerGroup,     // 按组取前N条记录
  MockScan,         // 模拟扫描（测试用）
  InitCheck,        // 初始化检查
  Window            // 窗口函数
};

class AbstractPlanNode;
using AbstractPlanNodeRef = std::shared_ptr<const AbstractPlanNode>;

/**
 * AbstractPlanNode 表示系统中所有可能的计划节点类型的基类。
 * 计划节点被建模为树结构，因此每个计划节点可以有任意数量的子节点。
 * 根据Volcano模型，计划节点接收其子节点产生的元组。
 * 子节点的顺序可能很重要。
 */
class AbstractPlanNode {
 public:
  /**
   * 使用指定的输出模式和子节点创建一个新的AbstractPlanNode。
   * @param output_schema 此计划节点的输出模式
   * @param children 此计划节点的子节点
   */
  AbstractPlanNode(SchemaRef output_schema, std::vector<AbstractPlanNodeRef> children)
      : output_schema_(std::move(output_schema)), children_(std::move(children)) {}

  /** 虚析构函数。 */
  virtual ~AbstractPlanNode() = default;

  /** @return 此计划节点的输出模式 */
  auto OutputSchema() const -> const Schema & { return *output_schema_; }

  /** @return 此计划节点在child_idx索引处的子节点 */
  auto GetChildAt(uint32_t child_idx) const -> AbstractPlanNodeRef { return children_[child_idx]; }

  /** @return 此计划节点的所有子节点 */
  auto GetChildren() const -> const std::vector<AbstractPlanNodeRef> & { return children_; }

  /** @return 此计划节点的类型 */
  virtual auto GetType() const -> PlanType = 0;

  /** @return 计划节点及其子节点的字符串表示 */
  auto ToString(bool with_schema = true) const -> std::string {
    if (with_schema) {
      return fmt::format("{} | {}{}", PlanNodeToString(), output_schema_, ChildrenToString(2, with_schema));
    }
    return fmt::format("{}{}", PlanNodeToString(), ChildrenToString(2, with_schema));
  }

  /** @return 带有新子节点的克隆计划节点 */
  virtual auto CloneWithChildren(std::vector<AbstractPlanNodeRef> children) const
      -> std::unique_ptr<AbstractPlanNode> = 0;

  /**
   * 此计划节点的输出模式。在Volcano模型中，每个计划节点都会产生元组，
   * 这里定义了此计划节点产生的元组所具有的模式。
   */
  SchemaRef output_schema_;

  /** 此计划节点的子节点。 */
  std::vector<AbstractPlanNodeRef> children_;

 protected:
  /** @return 计划节点自身的字符串表示 */
  virtual auto PlanNodeToString() const -> std::string { return "<unknown>"; }

  /** @return 计划节点子节点的字符串表示 */
  auto ChildrenToString(int indent, bool with_schema = true) const -> std::string;

 private:
};

}  // namespace bustub

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<bustub::AbstractPlanNode, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const T &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<bustub::AbstractPlanNode, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};