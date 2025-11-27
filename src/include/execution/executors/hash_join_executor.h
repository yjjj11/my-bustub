#pragma once

#include <functional>  // 用于std::hash
#include <memory>
#include <unordered_map>
#include <vector>
#include "common/exception.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"
#include "type/decimal_type.h"
#include "type/timestamp_type.h"
#include "type/value.h"          // 引入Value类
#include "type/value_factory.h"  // 引入空值生成工具
namespace bustub {

using JoinKey = std::vector<Value>;

inline size_t ComputeValueHash(const Value &val) {
  if (val.IsNull()) {
    return std::hash<int>()(0);  // 空值固定哈希值为0
  }
  switch (val.GetTypeId()) {
    case TypeId::INVALID:
      return std::hash<int>()(1);  // 无效类型固定哈希值
    case TypeId::BOOLEAN:
      return std::hash<bool>()(val.GetAs<bool>());
    case TypeId::TINYINT:
      return std::hash<int8_t>()(val.GetAs<int8_t>());
    case TypeId::SMALLINT:
      return std::hash<int16_t>()(val.GetAs<int16_t>());
    case TypeId::INTEGER:
      return std::hash<int32_t>()(val.GetAs<int32_t>());
    case TypeId::BIGINT:
      return std::hash<int64_t>()(val.GetAs<int64_t>());
    case TypeId::VARCHAR: {
      const char *data = val.GetData();
      uint32_t len = val.GetStorageSize();
      size_t hash = std::hash<uint32_t>()(len);
      for (uint32_t i = 0; i < len; ++i) {
        hash = hash * 31 + std::hash<char>()(data[i]);
      }
      return hash;
    }
    default:
      // 兜底：对未知类型，哈希其存储大小+原始地址（简化处理）
      return std::hash<uint32_t>()(val.GetStorageSize()) * 31 + std::hash<const void *>()(val.GetData());
  }
}

struct PartitionHash {
  size_t operator()(const JoinKey &key) const {
    size_t hash = 0;
    for (const auto &val : key) {
      // 组合每个Value的哈希值：乘质数31减少分布倾斜
      hash = hash * 31 + ComputeValueHash(val);
    }
    return hash;
  }
};

struct ProbeHash {
  size_t operator()(const JoinKey &key) const {
    size_t hash = 0;
    for (const auto &val : key) {
      hash ^= ComputeValueHash(val) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    return hash;
  }
};

struct KeyEquality {
  bool operator()(const JoinKey &a, const JoinKey &b) const {
    if (a.size() != b.size()) {
      return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
      // 使用Value的CompareEquals方法判断相等性
      if (a[i].CompareEquals(b[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

using ProbeHashTable = std::unordered_map<JoinKey, std::vector<Tuple>,
                                          ProbeHash,   // 探测阶段的哈希函数
                                          KeyEquality  // 连接键相等性判断
                                          >;

// ========================== 哈希连接执行器核心类 ==========================
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * 构造函数
   * @param exec_ctx 执行器上下文
   * @param plan 哈希连接计划节点
   * @param left_child 左表子执行器
   * @param right_child 右表子执行器
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** 初始化执行器：执行分区阶段，拆分左表/右表到磁盘 */
  void Init() override;

  /**
   * 生成下一批连接结果
   * @param tuple_batch 输出的元组批次
   * @param rid_batch 输出的RID批次（哈希连接无实际RID，可置空）
   * @param batch_size 批次大小
   * @return 是否生成了元组
   */
  auto Next(std::vector<Tuple> *tuple_batch, std::vector<RID> *rid_batch, size_t batch_size) -> bool override;

  /** @return 连接操作的输出Schema */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  // ========================== 核心辅助函数声明 ==========================
  /**
   * 生成元组的连接键
   * @param tuple 待生成键的元组
   * @param is_left 是否为左表元组
   * @return 元组对应的JoinKey
   */
  auto MakeJoinKey(const Tuple &tuple, bool is_left) const -> JoinKey;

  /**
   * 分区阶段：将表拆分为磁盘分区（使用PartitionHash）
   * @param child 待分区的子执行器
   * @param is_left 是否为左表
   */
  void PartitionTable(AbstractExecutor *child, bool is_left);

  /**
   * 构建阶段：为单个分区构建探测哈希表（使用ProbeHash）
   * @param partition_id 分区ID
   */
  void BuildProbeHashTable(size_t partition_id);

  /**
   * 探测阶段：用左表元组探测哈希表，生成连接结果
   * @param partition_id 分区ID
   * @param tuple_batch 输出的元组批次
   * @param batch_size 批次大小
   * @return 是否还有未处理的元组
   */
  void ProbePartition(size_t partition_id, std::vector<Tuple> *tuple_batch, size_t batch_size);

  void LoadTuplesFromPage(BufferPoolManager *bpm, page_id_t page_id, std::vector<Tuple> &tuples);

  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  // 分区管理：每个分区对应一组页ID（存储在IntermediateResultPage中）
  std::vector<std::vector<page_id_t>> left_partitions_;   // 左表分区：[分区ID][页ID列表]
  std::vector<std::vector<page_id_t>> right_partitions_;  // 右表分区：[分区ID][页ID列表]
  size_t num_partitions_;                                 // 分区数量（根据内存大小动态计算）

  // 执行状态：跟踪当前分区的处理进度
  size_t current_partition_;  // 当前处理的分区ID
  ProbeHashTable probe_ht_;   // 当前分区的探测哈希表（存储右表元组）
  size_t table_partition_;
  std::vector<Tuple> current_left_tuples_;  // 当前分区的左表元组缓存
  size_t left_tuple_idx_;                   // 左表元组的遍历索引
  size_t matched_right_idx_;                // 右表匹配元组的遍历索引
  bool partition_is_done_{false};
  // Schema缓存：避免重复获取
  const Schema &left_schema_;   // 左表的输出Schema
  const Schema &right_schema_;  // 右表的输出Schema
};

}  // namespace bustub