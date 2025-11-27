//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer_test.cpp
//
// 标识：test/buffer/lru_k_replacer_test.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库研究组
//
//===----------------------------------------------------------------------===//

/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest) {
  // 注意：与 `std::nullopt` 的比较结果始终为 `false`，如果 optional 类型实际包含值，
  // 则比较会针对内部的值进行。
  // 参考：https://devblogs.microsoft.com/oldnewthing/20211004-00/?p=105754
  std::optional<frame_id_t> frame;

  // 初始化替换器
  LRUKReplacer lru_replacer(7, 2);

  // 向替换器中添加6个帧。现在有帧 [1, 2, 3, 4, 5]。将帧6设为不可驱逐。
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  std::cout << "get here1\n";

  // 替换器的大小是可驱逐帧的数量，而不是已添加的总帧数。
  ASSERT_EQ(5, lru_replacer.Size());

  // 记录对帧1的访问。现在帧1总共有2次访问。
  lru_replacer.RecordAccess(1);
  // 其他所有帧现在共享最大的反向k距离。由于我们使用时间戳来打破平局，
  // 最先被驱逐的是时间戳最旧的帧，所以驱逐顺序应该是 [2, 3, 4, 5, 1]。

  // 从替换器中驱逐3个页。
  // 为了打破平局，我们使用基于最旧时间戳的LRU策略，即最近最少使用的帧。
  ASSERT_EQ(2, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(4, lru_replacer.Evict());
  ASSERT_EQ(2, lru_replacer.Size());
  // 现在替换器中有帧 [5, 1]。
  std::cout << "get heare2\n";

  // 插入新帧 [3, 4]，并更新帧5的访问历史。现在，顺序是 [3, 1, 5, 4]。
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());
  std::cout << "get heare3\n";

  // 查找下一个要驱逐的帧。预期帧3会被驱逐。
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Size());

  // 将6设为可驱逐。6应该是下一个被驱逐的，因为它有最大的反向k距离。
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  ASSERT_EQ(6, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Size());
  std::cout << "get heare4\n";

  // 将帧1标记为不可驱逐。现在我们有 [5, 4]。
  lru_replacer.SetEvictable(1, false);

  // 预期帧5会是下一个被驱逐的。
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(5, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Size());

  // 更新帧1的访问历史并将其设为可驱逐。现在我们有 [4, 1]。
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());

  // 驱逐最后两个帧。
  ASSERT_EQ(4, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Size());
  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(0, lru_replacer.Size());
  std::cout << "get heare5\n";

  // 再次插入帧1并将其标记为不可驱逐。
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(0, lru_replacer.Size());

  // 失败的驱逐不应改变替换器的大小。
  frame = lru_replacer.Evict();
  ASSERT_EQ(false, frame.has_value());
  std::cout << "get heare6\n";

  // 再次将帧1标记为可驱逐并驱逐它。
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(1, lru_replacer.Size());
  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(0, lru_replacer.Size());

  // 替换器中已经没有帧了，确保这不会导致异常行为。
  frame = lru_replacer.Evict();
  ASSERT_EQ(false, frame.has_value());
  ASSERT_EQ(0, lru_replacer.Size());
  std::cout << "get heare7\n";

  // 确保对不存在的帧设置可驱逐/不可驱逐状态不会导致异常行为。
  lru_replacer.SetEvictable(6, false);
  std::cout << "get heare8\n";
  lru_replacer.SetEvictable(6, true);
  std::cout << "get heare9\n";
}

}  // namespace bustub