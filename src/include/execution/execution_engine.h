//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_engine.h
//
// 标识：src/include/execution/execution_engine.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executor_factory.h"
#include "execution/executors/init_check_executor.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * ExecutionEngine 类负责执行查询计划。
 */
class ExecutionEngine {
 public:
  /**
   * 构造一个新的 ExecutionEngine 实例。
   * @param bpm 执行引擎使用的缓冲池管理器
   * @param txn_mgr 执行引擎使用的事务管理器
   * @param catalog 执行引擎使用的元数据管理器
   */
  ExecutionEngine(BufferPoolManager *bpm, TransactionManager *txn_mgr, Catalog *catalog)
      : bpm_{bpm}, txn_mgr_{txn_mgr}, catalog_{catalog} {}

  DISALLOW_COPY_AND_MOVE(ExecutionEngine);  // 禁用拷贝和移动构造

  /**
   * 执行查询计划。
   * @param plan 要执行的查询计划
   * @param result_set 执行计划后产生的元组集合
   * @param txn 执行查询所在的事务上下文
   * @param exec_ctx 执行查询所在的执行器上下文
   * @return 若查询计划执行成功则返回 `true`，否则返回 `false`
   */
  // NOLINTNEXTLINE
  auto Execute(const AbstractPlanNodeRef &plan, std::vector<Tuple> *result_set, Transaction *txn,
               ExecutorContext *exec_ctx) -> bool {
    BUSTUB_ASSERT((txn == exec_ctx->GetTransaction()), "不变式被破坏");

    // 为抽象计划节点构造对应的执行器
    auto executor = ExecutorFactory::CreateExecutor(exec_ctx, plan);

    // 初始化执行器
    auto executor_succeeded = true;

    try {
      executor->Init();
      PollExecutor(executor.get(), plan, result_set);
      PerformChecks(exec_ctx);
    } catch (const ExecutionException &ex) {
      executor_succeeded = false;
      if (result_set != nullptr) {
        result_set->clear();
      }
    }

    return executor_succeeded;
  }

  /**
   * 执行执行过程中的正确性检查
   * @param exec_ctx 执行器上下文
   */
  void PerformChecks(ExecutorContext *exec_ctx) {
    for (const auto &[left_executor, right_executor] : exec_ctx->GetNLJCheckExecutorSet()) {
      auto casted_left_executor = dynamic_cast<const InitCheckExecutor *>(left_executor);
      auto casted_right_executor = dynamic_cast<const InitCheckExecutor *>(right_executor);
      BUSTUB_ASSERT(casted_right_executor->GetInitCount() + 1 >= casted_left_executor->GetNextCount(),
                    "嵌套循环连接检查失败，是否在每次处理左表元组时都初始化了右表执行器？（差1是允许的）");
    }
  }

 private:
  /**
   * 轮询执行器直到数据耗尽或抛出异常
   * @param executor 根执行器
   * @param plan 要执行的查询计划
   * @param result_set 元组结果集
   */
  static void PollExecutor(AbstractExecutor *executor, const AbstractPlanNodeRef &plan,
                           std::vector<Tuple> *result_set) {
    std::vector<RID> rids{};
    std::vector<Tuple> tuples{};
    while (executor->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {  // 批量获取结果
      if (result_set != nullptr) {
        result_set->insert(result_set->end(), tuples.begin(), tuples.end());  // 将批量结果插入结果集
      }
    }
  }

  [[maybe_unused]] BufferPoolManager *bpm_;       // 缓冲池管理器（可能未使用）
  [[maybe_unused]] TransactionManager *txn_mgr_;  // 事务管理器（可能未使用）
  [[maybe_unused]] Catalog *catalog_;             // 元数据管理器（可能未使用）
};

}  // namespace bustub