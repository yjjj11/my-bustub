//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// executor_context.h
//
// 标识：src/include/execution/executor_context.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/check_options.h"
#include "execution/executors/abstract_executor.h"
#include "storage/page/tmp_tuple_page.h"

namespace bustub {
class AbstractExecutor;

/**
 * ExecutorContext 存储执行器运行所需的所有上下文信息。
 */
class ExecutorContext {
 public:
  /**
   * 为执行查询的事务创建 ExecutorContext。
   * @param transaction 执行查询的事务
   * @param catalog 执行器使用的元数据管理器
   * @param bpm 执行器使用的缓冲池管理器
   * @param txn_mgr 执行器使用的事务管理器
   * @param lock_mgr 执行器使用的锁管理器
   */
  ExecutorContext(Transaction *transaction, Catalog *catalog, BufferPoolManager *bpm, TransactionManager *txn_mgr,
                  LockManager *lock_mgr, bool is_delete)
      : transaction_(transaction),
        catalog_{catalog},
        bpm_{bpm},
        txn_mgr_(txn_mgr),
        lock_mgr_(lock_mgr),
        is_delete_(is_delete) {
    nlj_check_exec_set_ = std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>>(
        std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>>{});
    check_options_ = std::make_shared<CheckOptions>();
  }

  ~ExecutorContext() = default;

  DISALLOW_COPY_AND_MOVE(ExecutorContext);  // 禁用拷贝和移动构造

  /** @return 正在运行的事务 */
  auto GetTransaction() const -> Transaction * { return transaction_; }

  /** @return 元数据管理器 */
  auto GetCatalog() -> Catalog * { return catalog_; }

  /** @return 缓冲池池管理器 */
  auto GetBufferPoolManager() -> BufferPoolManager * { return bpm_; }

  /** @return 日志管理器 - 目前无需关注 */
  auto GetLogManager() -> LogManager * { return nullptr; }

  /** @return 锁管理器 */
  auto GetLockManager() -> LockManager * { return lock_mgr_; }

  /** @return 事务管理器 */
  auto GetTransactionManager() -> TransactionManager * { return txn_mgr_; }

  /** @return 嵌套循环连接检查执行器集合 */
  auto GetNLJCheckExecutorSet() -> std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>> & {
    return nlj_check_exec_set_;
  }

  /** @return 检查选项 */
  auto GetCheckOptions() -> std::shared_ptr<CheckOptions> { return check_options_; }

  /** 添加用于检查的执行器对（左执行器和右执行器） */
  void AddCheckExecutor(AbstractExecutor *left_exec, AbstractExecutor *right_exec) {
    nlj_check_exec_set_.emplace_back(left_exec, right_exec);
  }

  /** 初始化检查选项 */
  void InitCheckOptions(std::shared_ptr<CheckOptions> &&check_options) {
    BUSTUB_ASSERT(check_options, "不能为nullptr");
    check_options_ = std::move(check_options);
  }

  /** 截至2023年秋季，此函数不应被使用 */
  auto IsDelete() const -> bool { return is_delete_; }

 private:
  /** 与此执行器上下文关联的事务上下文 */
  Transaction *transaction_;
  /** 与此执行器上下文关联的数据库元数据管理器 */
  Catalog *catalog_;
  /** 与此执行器上下文关联的缓冲池管理器 */
  BufferPoolManager *bpm_;
  /** 与此执行器上下文关联的事务管理器 */
  TransactionManager *txn_mgr_;
  /** 与此执行器上下文关联的锁管理器 */
  LockManager *lock_mgr_;
  /** 与此执行器上下文关联的嵌套循环连接检查执行器集合 */
  std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>> nlj_check_exec_set_;
  /** 与此执行器上下文关联的检查选项集合 */
  std::shared_ptr<CheckOptions> check_options_;
  bool is_delete_;  // 标记是否为删除操作（暂未使用）
};

}  // namespace bustub