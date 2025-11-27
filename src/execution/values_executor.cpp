//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// values_executor.cpp
//
// 标识：src/execution/executors/values_executor.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include "execution/executors/values_executor.h"

namespace bustub {

/**
 * 构造一个新的 ValuesExecutor 实例。
 * @param exec_ctx 执行器上下文（包含数据库连接、事务等信息）
 * @param plan 要执行的 values 计划节点（存储待输出的常量数据）
 */
ValuesExecutor::ValuesExecutor(ExecutorContext *exec_ctx, const ValuesPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), dummy_schema_(Schema({})) {}

/** 初始化 values 执行器 */
void ValuesExecutor::Init() { cursor_ = 0; }  // 重置游标，从第0行开始输出

/**
 * 从 values 计划中生成下一批元组。
 * @param[out] tuple_batch 本次输出的元组批（存放实际数据）
 * @param[out] rid_batch 本次输出的元组对应的RID批（values无实际存储，用空RID填充）
 * @param batch_size 批处理大小（一次最多输出的元组数量，默认值为BUSTUB_BATCH_SIZE）
 * @return 若生成了元组则返回true，若所有数据已输出则返回false
 */
auto ValuesExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();  // 清空输出容器
  rid_batch->clear();

  // 循环填充批处理，直到达到batch_size或数据耗尽
  while (tuple_batch->size() < batch_size && cursor_ < plan_->GetValues().size()) {
    std::vector<Value> values{};                         // 存储一行数据的所有列值
    values.reserve(GetOutputSchema().GetColumnCount());  // 预分配空间

    const auto &row_expr = plan_->GetValues()[cursor_];  // 获取当前行的表达式列表
    for (const auto &col : row_expr) {
      // 计算表达式的值（values是常量，无需输入元组和schema，用空指针和空schema即可）
      values.push_back(col->Evaluate(nullptr, dummy_schema_));
    }

    // 将计算出的行值包装成元组，添加到批处理中（元组结构由输出schema定义）
    tuple_batch->emplace_back(values, &GetOutputSchema());
    rid_batch->emplace_back(RID{});  // values无实际存储位置，用空RID占位
    cursor_ += 1;                    // 游标移动到下一行
  }

  // 若批处理不为空，说明有数据输出，返回true；否则返回false
  return !tuple_batch->empty();
}

}  // namespace bustub