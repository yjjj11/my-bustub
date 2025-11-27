//===----------------------------------------------------------------------===//
//
//                         BusTub 数据库
//
// hash_join_executor.cpp
//
// 标识：src/execution/hash_join_executor.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"
#include "fmt/core.h"  // 用于格式化错误信息
#include "storage/page/page_guard.h"
#include "type/type_id.h"
namespace bustub {

/**
 * 构造 HashJoinExecutor 实例。
 * @param exec_ctx 执行器上下文
 * @param plan 要执行的哈希连接计划
 * @param left_child 为连接左侧生成元组的子执行器
 * @param right_child 为连接右侧生成元组的子执行器
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),            // 调用父类构造函数初始化执行器上下文
      plan_(plan),                           // 哈希连接计划节点
      left_child_(std::move(left_child)),    // 左表子执行器（移动语义）
      right_child_(std::move(right_child)),  // 右表子执行器（移动语义）
      num_partitions_(8),                    // 分区数量（简化为固定值，可根据内存动态调整）
      current_partition_(0),                 // 初始化当前处理的分区ID为0
      left_tuple_idx_(0),                    // 左表元组遍历索引初始化为0
      matched_right_idx_(0),                 // 右表匹配元组索引初始化为0
      left_schema_(plan->GetLeftPlan()->OutputSchema()),   // 左表输出Schema缓存
      right_schema_(plan->GetRightPlan()->OutputSchema())  // 右表输出Schema缓存
{
  // 校验连接类型：仅支持内连接和左外连接
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    throw NotImplementedException(
        fmt::format("哈希连接暂不支持该连接类型，仅支持INNER/LEFT：{}", static_cast<int>(plan->GetJoinType())));
  }
  std::cout << "进入了HashJoinExecutor\n";

  // 初始化分区存储结构：为每个分区预留页ID列表的空间
  left_partitions_.resize(num_partitions_);
  right_partitions_.resize(num_partitions_);
}

// 提取对应表达式的值
auto HashJoinExecutor::MakeJoinKey(const Tuple &tuple, bool is_left) const -> JoinKey {
  JoinKey join_key;
  const auto &expressions = is_left ? plan_->LeftJoinKeyExpressions() : plan_->RightJoinKeyExpressions();
  const auto &target_schema = is_left ? left_schema_ : right_schema_;

  // 遍历所有连接键表达式，计算每个表达式的值并加入连接键
  for (const auto &expr : expressions) {
    // 对元组执行表达式求值，得到单个属性的Value
    Value expr_value = expr->Evaluate(&tuple, target_schema);
    join_key.emplace_back(std::move(expr_value));
  }

  return join_key;
}

// 接 MakeJoinKey 后的代码，继续在 namespace bustub 中实现
/**
 * 分区阶段：将表拆分为磁盘分区（使用PartitionHash计算分区ID）
 * @param child 待分区的子执行器（左表/右表的扫描执行器）
 * @param is_left 是否为左表元组（用于区分左/右分区存储）
 */
void HashJoinExecutor::PartitionTable(AbstractExecutor *child, bool is_left) {
  // 1. 获取缓冲池管理器（用于创建/读写页）
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  if (bpm == nullptr) {
    throw Exception("哈希连接分区失败：缓冲池管理器为空");
  }

  // 2. 绑定当前表的Schema和分区存储结构
  auto &partitions = is_left ? left_partitions_ : right_partitions_;
  PartitionHash partition_hash;  // 分区哈希函数（用于计算元组的分区ID）

  // 3. 批次读取子执行器的元组（按BUSTUB_BATCH_SIZE批量处理，提升性能）
  std::vector<Tuple> tuples;
  std::vector<RID> rids;  // 哈希连接不依赖RID，仅为适配Next接口
  while (child->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      // 3.1 生成当前元组的连接键
      JoinKey join_key = MakeJoinKey(tuple, is_left);

      // 3.2 计算分区ID：分区哈希值对分区数量取模，确保分区ID在[0, num_partitions_)范围内
      size_t partition_idx = partition_hash(join_key) % num_partitions_;

      // 3.3 获取/创建当前分区的页（优先使用分区的最后一页，满则新建）
      page_id_t page_id;
      if (partitions[partition_idx].empty()) {
        // 分区为空，创建新页并加入分区的页列表
        page_id = bpm->NewPage();
        if (page_id == INVALID_PAGE_ID) {
          throw Exception(fmt::format("哈希连接分区失败：创建新页失败，分区ID={}", partition_idx));
        }
        partitions[partition_idx].push_back(page_id);
      } else {
        // 分区已有页，使用最后一页（减少页切换开销）
        page_id = partitions[partition_idx].back();
      }

      // 3.4 将元组写入当前页的IntermediateResultPage
      WritePageGuard guard = bpm->WritePage(page_id);             // 获取写页锁
      auto *result_page = guard.AsMut<IntermediateResultPage>();  // 转换为自定义结果页

      // 初始化页的元数据（首次写入时执行）
      if (result_page->get_tuple_count() == 0) {
        result_page->InitMetadata(BUSTUB_PAGE_SIZE);
      }

      // 3.5 若当前页已满，创建新页并重新写入
      if (!result_page->WriteTuple(tuple)) {
        guard.Drop();  // 释放当前页的写锁（避免内存泄漏）

        // 创建新页并加入分区
        page_id = bpm->NewPage();
        if (page_id == INVALID_PAGE_ID) {
          throw Exception(fmt::format("哈希连接分区失败：页满后创建新页失败，分区ID={}", partition_idx));
        }
        partitions[partition_idx].push_back(page_id);

        // 将元组写入新页
        WritePageGuard new_guard = bpm->WritePage(page_id);
        auto *new_page = new_guard.AsMut<IntermediateResultPage>();
        new_page->InitMetadata(BUSTUB_PAGE_SIZE);
        new_page->WriteTuple(tuple);  // 新页一定有空间，无需判断返回值
      }
    }

    // 清空批次数据，准备下一批读取
    tuples.clear();
    rids.clear();
  }
}

// 接 PartitionTable 后的代码，继续在 namespace bustub 中实现
/**
 * 构建阶段：为单个分区构建探测哈希表（仅处理右表元组）
 * @param partition_id 待构建哈希表的分区ID
 */
void HashJoinExecutor::BuildProbeHashTable(size_t partition_id) {
  // 1. 清空上一个分区的探测哈希表，避免数据残留
  probe_ht_.clear();

  std::cout << "进入了哈希表构建阶段\n";
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  if (bpm == nullptr) {
    throw Exception("哈希连接构建失败：缓冲池管理器为空");
  }

  // 3. 获取当前分区的右表页列表（分区阶段已将右表元组写入这些页）
  const auto &right_page_ids = right_partitions_[partition_id];
  if (right_page_ids.empty()) {
    return;  // 右表该分区无元组，无需构建哈希表
  }

  // 4. 遍历右表分区的所有页，读取元组并插入探测哈希表
  for (page_id_t page_id : right_page_ids) {
    // 4.1 获取读页锁，转换为IntermediateResultPage（const版本）
    ReadPageGuard guard = bpm->ReadPage(page_id);
    std::cout << "当前分区页有" << guard.GetPageId() << "\n";
    const auto *result_page = guard.As<IntermediateResultPage>();

    // 4.2 遍历页内所有元组（按元组数量循环）
    const size_t tuple_count = result_page->get_tuple_count();
    for (size_t tuple_idx = 0; tuple_idx < tuple_count; ++tuple_idx) {
      // 4.3 定义空元组，通过引用传入ReadTuple读取数据
      Tuple right_tuple;
      bool read_success = result_page->ReadTuple(tuple_idx, right_tuple);
      if (!read_success) {
        // 元组读取失败（索引越界/页数据损坏），跳过当前元组
        continue;
      }

      // 4.4 生成右表元组的连接键（is_left=false 表示右表）
      JoinKey join_key = MakeJoinKey(right_tuple, false);

      // 4.5 将元组插入探测哈希表：相同连接键的元组存入同一个列表
      probe_ht_[join_key].push_back(right_tuple);
    }
  }
}

void HashJoinExecutor::Init() {
  std::cout << "Into Init\n";
  try {
    left_child_->Init();
    right_child_->Init();

    PartitionTable(left_child_.get(), true);
    std::cout << "After PartitionTable(left_child_.get(), true);\n";
    PartitionTable(right_child_.get(), false);
    std::cout << "After PartitionTable(righrt,false);\n";
  } catch (const Exception &e) {
    throw Exception(fmt::format("哈希连接初始化失败：{}", e.what()));
  }

  current_partition_ = 0;
  left_tuple_idx_ = 0;
  matched_right_idx_ = 0;
  probe_ht_.clear();
  current_left_tuples_.clear();
  table_partition_ = 0;
  std::cout << "Init Successfully\n";
}

// hash_join_executor.cpp
/**
 * 从指定页加载所有元组并写入外部vector（传入缓冲池指针，避免锁竞争）
 * @param bpm 缓冲池管理器指针（外部已获取）
 * @param page_id 页ID
 * @param tuples 外部vector，接收页内元组
 */
void HashJoinExecutor::LoadTuplesFromPage(BufferPoolManager *bpm, page_id_t page_id, std::vector<Tuple> &tuples) {
  // 前置校验：缓冲池指针不能为空
  if (bpm == nullptr) {
    throw Exception("加载页元组失败：缓冲池管理器指针为空");
  }

  // 直接使用外部传入的缓冲池获取页锁，避免重复获取资源
  ReadPageGuard guard = bpm->ReadPage(page_id, AccessType::Unknown);
  const auto *result_page = guard.As<IntermediateResultPage>();

  // 遍历页内所有元组，调用ReadTuple读取并写入外部vector
  const size_t tuple_count = result_page->get_tuple_count();
  for (size_t tuple_idx = 0; tuple_idx < tuple_count; ++tuple_idx) {
    Tuple left_tuple;
    bool read_success = result_page->ReadTuple(tuple_idx, left_tuple);
    if (read_success) {
      tuples.push_back(left_tuple);
      // std::cout<<"左表元组now="<<left_tuple.ToString(&left_schema_)<<"\n";
    }
  }
}

/**
 * 探测阶段：用左表元组探测哈希表，生成连接结果（改为void，直接填充批次）
 * @param partition_id 分区ID
 * @param tuple_batch 输出的元组批次（需提前非空/已有部分数据）
 * @param batch_size 批次大小（单次最多返回的元组数量）
 */
void HashJoinExecutor::ProbePartition(size_t partition_id, std::vector<Tuple> *tuple_batch, size_t batch_size) {
  // 前置校验：确保输出批次指针有效
  if (tuple_batch == nullptr) {
    throw Exception("哈希连接探测失败：输出批次指针为空");
  }

  // 仅获取一次缓冲池指针，后续所有操作复用该指针（核心优化）
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  if (bpm == nullptr) {
    throw Exception("哈希连接探测失败：缓冲池管理器为空");
  }
  // std::cout<<"进入了ProbePartition\n";
  // 1. 若当前左表元组缓存为空，从磁盘分区加载左表元组
  if (left_tuple_idx_ >= current_left_tuples_.size()) {
    current_left_tuples_.clear();  // 清空旧缓存
    left_tuple_idx_ = 0;           // 重置遍历索引
    matched_right_idx_ = 0;        // 重置右表匹配索引

    // 1.1 获取当前分区的左表页列表
    const auto &left_page_ids = left_partitions_[partition_id];
    if (left_page_ids.empty()) {
      return;  // 左表该分区无元组，直接返回
    }

    // std::cout<<"遍历左表页，调用封装函数加载所有元组到缓存【传入缓冲池指针】\n";
    for (page_id_t page_id : left_page_ids) {
      LoadTuplesFromPage(bpm, page_id, current_left_tuples_);  // 核心修改：传入bpm
    }

    // 1.3 若加载后缓存仍为空，说明左表该分区无元组
    if (current_left_tuples_.empty()) {
      return;
    }
  }

  // 2. 遍历左表元组缓存，匹配右表元组并生成结果
  const size_t total_left_tuples = current_left_tuples_.size();
  std::cout << "total_left_tuples=" << total_left_tuples << "\n";
  while (left_tuple_idx_ < total_left_tuples) {
    if (tuple_batch->size() >= batch_size) {
      break;
    }
    // 2.3 查询探测哈希表，找到匹配的右表元组列表
    const Tuple &left_tuple = current_left_tuples_[left_tuple_idx_];

    // std::cout<<"当前left_tuple="<<left_tuple.ToString(&left_schema_);
    JoinKey left_join_key = MakeJoinKey(left_tuple, true);
    // std::cout<<"make joinkey\n";
    auto it = probe_ht_.find(left_join_key);

    if (it != probe_ht_.end()) {
      // std::cout<<"3.1 找到匹配的右表元组列表\n";
      const auto &right_tuples = it->second;
      const size_t total_right_tuples = right_tuples.size();
      // std::cout<<"数量为"<<total_right_tuples<<"\n";
      // 遍历匹配的右表元组，生成连接结果
      while (matched_right_idx_ < total_right_tuples) {
        // 批次已满，直接退出所有循环
        if (tuple_batch->size() >= batch_size) {
          std::cout << "从这里出去\n";
          break;
        }

        const Tuple &right_tuple = right_tuples[matched_right_idx_];
        // std::cout<<"当前right_tuple="<<right_tuple.ToString(&right_schema_)<<"\n";
        // std::cout<<batch_size<<"\n";
        // 拼接左表+右表字段，生成结果元组
        std::vector<Value> result_values;
        // 先加左表所有字段
        for (size_t i = 0; i < left_schema_.GetColumns().size(); i++) {
          result_values.push_back(left_tuple.GetValue(&left_schema_, i));
        }
        // 再加右表所有字段
        for (size_t i = 0; i < right_schema_.GetColumns().size(); i++) {
          result_values.push_back(right_tuple.GetValue(&right_schema_, i));
        }
        // 构造结果元组（使用输出Schema）
        Tuple result_tuple(result_values, &plan_->OutputSchema());
        // std::cout<<"当前result_tuple="<<result_tuple.ToString(&plan_->OutputSchema())<<"\n";
        tuple_batch->push_back(result_tuple);
        // std::cout<<"tuple_batch的大小为"<<tuple_batch->size()<<"\n";
        matched_right_idx_++;  // 右表匹配索引+1
        // std::cout<<"matched_right_idx_="<<matched_right_idx_<<"\n\n";
        if (matched_right_idx_ >= total_right_tuples) {
          matched_right_idx_ = 0;
          left_tuple_idx_++;
          // std::cout<<"左表元组索引递增：left_tuple_idx_ = "<<left_tuple_idx_<<"\n";
          break;
        }
      }

      // 3.2 若右表元组遍历完毕，重置右表索引，左表索引+1

    } else {
      // 3.3 未找到匹配的右表元组：左外连接需补空值，内连接直接跳过
      if (plan_->GetJoinType() == JoinType::LEFT) {
        if (tuple_batch->size() >= batch_size) {
          break;
        }
        std::vector<Value> result_values;
        for (size_t i = 0; i < left_schema_.GetColumns().size(); i++) {
          result_values.push_back(left_tuple.GetValue(&left_schema_, i));
        }
        for (const auto &col : right_schema_.GetColumns()) {
          result_values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
        Tuple result_tuple(result_values, &plan_->OutputSchema());
        tuple_batch->push_back(result_tuple);
      }

      // 左表索引+1，重置右表匹配索引
      left_tuple_idx_++;
      matched_right_idx_ = 0;
    }
  }
  if (left_tuple_idx_ >= total_left_tuples) {
    current_left_tuples_.clear();
    left_tuple_idx_ = 0;
    partition_is_done_ = true;
    std::cout << "左表元组处理完毕，清空缓存\n";
  }
}

/**
 * 哈希连接执行器核心读取接口：按分区处理生成连接结果，适配含RID的接口规范
 * @param tuple_batch 输出的元组批次（需提前清空）
 * @param rid_batch 输出的RID批次（哈希连接无有效RID，仅做空填充）
 * @param batch_size 单次返回的最大元组数量
 * @return 是否成功获取到元组（true=有结果，false=所有数据处理完毕）
 */
auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  // std::cout<<"调用了next\n";
  // 2. 循环处理所有分区，直到批次填满或分区遍历完毕
  while (current_partition_ < num_partitions_) {
    std::cout << "当前分区为" << current_partition_ << "\n";
    const size_t partition_id = current_partition_;

    // 仅在分区切换时构建哈希表
    // if (table_partition_ != partition_id) {
    BuildProbeHashTable(partition_id);
    table_partition_ = partition_id;
    // partition_is_done_ = false;  // 新分区初始化为未处理
    // }

    // 探测当前分区，填充批次
    ProbePartition(partition_id, tuple_batch, batch_size);

    // 批次填满或有数据时的处理
    if (!tuple_batch->empty()) {
      // 若分区未处理完，返回当前批次（保留状态）
      if (!partition_is_done_) {
        rid_batch->resize(tuple_batch->size());
        return true;
      }
      // 若分区已处理完，推进分区并继续处理下一个
      current_partition_++;
      left_tuple_idx_ = 0;
      matched_right_idx_ = 0;
      current_left_tuples_.clear();
      probe_ht_.clear();
      table_partition_ = INVALID_PAGE_ID;  // 重置哈希表标记
      partition_is_done_ = false;
      // 若批次仍有数据（未填满），返回结果
      if (!tuple_batch->empty()) {
        rid_batch->resize(tuple_batch->size());
        return true;
      }
    } else {
      // 分区无数据，直接推进
      current_partition_++;
      table_partition_ = INVALID_PAGE_ID;
    }
  }

  // 8. 所有分区处理完毕：若批次有数据（未填满）仍返回true，否则返回false
  const bool has_data = !tuple_batch->empty();
  if (has_data) {
    rid_batch->resize(tuple_batch->size());
  }
  std::cout << "这次返回了" << batch_size << "个数据\n";
  return has_data;
}
}  // namespace bustub