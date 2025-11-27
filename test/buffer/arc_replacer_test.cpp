//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer_test.cpp
//
// Identification: test/buffer/arc_replacer_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * arc_replacer_test.cpp
 */

#include "buffer/arc_replacer.h"

#include "gtest/gtest.h"

namespace bustub {

TEST(ArcReplacerTest, SampleTest) {
  // 为了简单起见
  // 我们使用 (a, fb) 表示页面a在帧b上，
  // (a, _) 表示页面id为a的幽灵页面
  // p(a, fb) 表示在帧b上被固定的页面a
  // 我们使用 [<-mru_ghost-][<-mru-]![-mfu->][->mfu_ghost->] p=x
  // 来表示4个列表，其中靠近!的页面更新鲜
  // 并记录MRU目标大小为x
  std::cout << "开始测试1了\n";
  ArcReplacer arc_replacer(7);
  std::cout << "设置帧的最大大小为7\n";

  // 向替换器添加六个帧。
  // 我们将帧6设置为不可驱逐。这些页面都进入mru列表
  // 现在我们有帧 [][(1,f1), (2,f2), (3,f3), (4,f4), (5,f5), p(6,f6)]![][]
  arc_replacer.RecordAccess(1, 1);
  arc_replacer.RecordAccess(2, 2);
  arc_replacer.RecordAccess(3, 3);
  arc_replacer.RecordAccess(4, 4);
  arc_replacer.RecordAccess(5, 5);
  arc_replacer.RecordAccess(6, 6);
  arc_replacer.SetEvictable(1, true);
  arc_replacer.SetEvictable(2, true);
  arc_replacer.SetEvictable(3, true);
  arc_replacer.SetEvictable(4, true);
  arc_replacer.SetEvictable(5, true);
  arc_replacer.SetEvictable(6, false);

  // 替换器的大小是可以被驱逐的帧数量，_不是_输入的总帧数。
  ASSERT_EQ(5, arc_replacer.Size());
  std::cout << "目前的可驱逐大小为5\n";

  // 记录帧1的访问。现在帧1进入mfu列表
  arc_replacer.RecordAccess(1, 1);
  // 现在 [][(2,f2), (3,f3), (4,f4), (5,f5), p(6,f6)]![(1,f1)][] p=0
  //
  // 现在从替换器中驱逐三个页面。
  // 由于目标大小仍然是0，应该从mru侧驱逐
  ASSERT_EQ(2, arc_replacer.Evict());
  ASSERT_EQ(3, arc_replacer.Evict());
  ASSERT_EQ(4, arc_replacer.Evict());
  ASSERT_EQ(2, arc_replacer.Size());
  // 现在 [(2,_), (3,_), (4,_)][(5,f5), p(6,f6)]![(1,f1)][] p=0
  std::cout << "现在驱逐了三个页面，还剩下两个改革可驱逐\n";
  // 插入新页面7到帧2，这应该_不是_幽灵列表的命中
  // 因为我们从未见过页面7，这进入mru列表
  arc_replacer.RecordAccess(2, 7);
  arc_replacer.SetEvictable(2, true);
  std::cout << "插入了页面7到mru\n";
  // 插入页面2到帧3，这应该是mru幽灵列表的命中
  // 因为我们刚刚驱逐了页面2，这进入mfu列表
  // 同时目标大小应该增加1，因为mru幽灵有
  // 大小3而mfu幽灵有大小0
  // 以x开头的表示幽灵列表中的页面id，以_开头的表示被固定
  arc_replacer.RecordAccess(3, 2);
  arc_replacer.SetEvictable(3, true);
  std::cout << "将2从mru幽灵中加入到mfu中，目标量++\n";
  // 现在 [(3,_), (4,_)][(5,f5), p(6,f6), (7,f2)]![(2,f3), (1,f1)][] p=1
  ASSERT_EQ(4, arc_replacer.Size());
  std::cout << "目前的可驱逐大小为4\n";
  // 继续插入页面3到帧4和页面4到帧7
  arc_replacer.RecordAccess(4, 3);
  std::cout << "arc_replacer.RecordAccess(4, 3) done\n";
  arc_replacer.SetEvictable(4, true);
  std::cout << "arc_replacer.SetEvictable(4, true) done\n";
  arc_replacer.RecordAccess(7, 4);
  std::cout << "arc_replacer.RecordAccess(7, 4) done\n";
  arc_replacer.SetEvictable(7, true);
  std::cout << "arc_replacer.SetEvictable(7, true); done\n";
  // 现在 [][(5,f5), p(6,f6), (7,f2)]![(4,f7), (3,f4), (2,f3), (1,f1)][] p=3
  ASSERT_EQ(6, arc_replacer.Size());
  std::cout << "目前的可驱逐大小为6\n";
  // 驱逐一个条目，现在目标大小是3，我们仍然从mru驱逐
  ASSERT_EQ(5, arc_replacer.Evict());
  // 现在 [(5,_)][p(6,f6), (7,f2)]![(4,f7), (3,f4), (2,f3), (1,f1)][] p=3
  // 驱逐另一个条目，这次mru小于目标大小，
  // mfu成为受害者
  std::cout << "驱逐mru的5\n";
  ASSERT_EQ(1, arc_replacer.Evict());
  // 现在 [(5,_)][p(6,f6), (7,f2)]![(4,f7), (3,f4), (2,f3)][(1,_)] p=3
  std::cout << "驱逐mfu的1";
  // 再次访问页面1到帧5，现在页面1回到mfu
  // 使用不同的帧，同时p根据1/1=1调整减少
  arc_replacer.RecordAccess(5, 1);
  arc_replacer.SetEvictable(5, true);
  // 现在 [(5,_)][p(6,f6), (7,f2)]![(1,f5), (4,f7), (3,f4), (2,f3)][] p=2
  ASSERT_EQ(5, arc_replacer.Size());

  // 我们再次驱逐，这次目标大小是2，我们从mru驱逐，
  // 注意页面6被固定。受害者是页面7
  // 现在 [(5,_), (7,_)][p(6,f6)]![(1,f5), (4,f7), (3,f4), (2,f3)][] p=2
  ASSERT_EQ(2, arc_replacer.Evict());
}

TEST(ArcReplacerTest, SampleTest2) {
  // 测试较小的容量
  ArcReplacer arc_replacer(3);
  // 填满替换器
  arc_replacer.RecordAccess(1, 1);
  arc_replacer.SetEvictable(1, true);
  arc_replacer.RecordAccess(2, 2);
  arc_replacer.SetEvictable(2, true);
  arc_replacer.RecordAccess(3, 3);
  arc_replacer.SetEvictable(3, true);
  ASSERT_EQ(3, arc_replacer.Size());
  // 现在 [][(1,f1), (2,f2), (3,f3)]![][] p=0
  // 驱逐所有页面
  ASSERT_EQ(1, arc_replacer.Evict());
  ASSERT_EQ(2, arc_replacer.Evict());
  ASSERT_EQ(3, arc_replacer.Evict());
  ASSERT_EQ(0, arc_replacer.Size());
  // 现在 [(1,_), (2,_), (3,_)][]![][] p=0

  // 插入新页面4到帧3。这是情况4A
  // 幽灵页面1应该被驱逐出去
  arc_replacer.RecordAccess(3, 4);
  arc_replacer.SetEvictable(3, true);
  // 现在 [(2,_), (3,_)][(4,f3)]![][] p=0

  // 访问页面1到帧2，它应该_不是_幽灵列表的命中。
  // 幽灵页面2应该被驱逐出去
  arc_replacer.RecordAccess(2, 1);
  arc_replacer.SetEvictable(2, true);
  ASSERT_EQ(2, arc_replacer.Size());
  // 现在 [(3,_)][(4,f3), (1,f2)]![][] p=0

  // 访问页面3到帧1，这应该是幽灵命中，
  // 页面3被放置在mfu上，目标大小增加1
  arc_replacer.RecordAccess(1, 3);
  arc_replacer.SetEvictable(1, true);
  // 现在 [][(4,f3), (1,f2)]![(3,f1)][] p=1

  // 通过再次驱逐所有页面创建更多幽灵
  ASSERT_EQ(3, arc_replacer.Evict());
  ASSERT_EQ(2, arc_replacer.Evict());
  ASSERT_EQ(1, arc_replacer.Evict());
  // 现在 [(4,_), (1,_)][]![][(3,_)] p=1

  // 让我们创建更多幽灵来填满列表到"满"
  // 再次插入页面1使其进入mfu侧，
  // 目标增加1
  arc_replacer.RecordAccess(1, 1);
  arc_replacer.SetEvictable(1, true);
  // 现在 [(4,_)][]![(1,f1)][(3,_)] p=2

  // 再次插入页面4使其进入mfu侧，
  // 目标增加1
  arc_replacer.RecordAccess(2, 4);
  arc_replacer.SetEvictable(2, true);
  // 现在 [][]![(4,f2),(1,f1)][(3,_)] p=3

  // 现在一次插入和驱逐一个新页面
  // 插入页面5并驱逐，由于目标大小是3，
  // 应该驱逐页面1
  arc_replacer.RecordAccess(3, 5);
  arc_replacer.SetEvictable(3, true);
  ASSERT_EQ(1, arc_replacer.Evict());
  // 现在 [][(5,f3)]![(4,f2)][(1,_),(3,_)] p=3
  // 插入页面6并驱逐，注意目标大小是3，
  // 所以页面4被驱逐
  arc_replacer.RecordAccess(1, 6);
  arc_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, arc_replacer.Evict());
  // 现在 [][(5,f3),(6,f1)]![(4,_),(1,_),(3,_)] p=3
  // 插入页面7并驱逐，注意目标大小是3，
  // 所以页面5被驱逐
  arc_replacer.RecordAccess(2, 7);
  arc_replacer.SetEvictable(2, true);
  ASSERT_EQ(3, arc_replacer.Evict());
  // 现在 [(5,_)][(6,f1),(7,f2)]![][(4,_),(1,_),(3,_)] p=3

  // 现在列表满了！达到2*容量
  // 将页面5调整到mfu列表
  arc_replacer.RecordAccess(3, 5);
  arc_replacer.SetEvictable(3, true);
  // 现在 [][(6,f1),(7,f2)]![(5,f3)][(4,_),(1,_),(3,_)] p=3

  // 现在驱逐，目标应该是mfu
  ASSERT_EQ(3, arc_replacer.Evict());
  // 现在 [][(6,f1),(7,f2)]![][(5,_),(4,_),(1,_),(3,_)] p=3

  // 现在mru和mru_ghost一起有
  // 少于3条记录。当插入新页面2时
  // 这应该是情况4B并且
  // 四个列表总大小等于2 * 容量的情况，
  // 所以mfu幽灵将被收缩
  arc_replacer.RecordAccess(3, 2);
  arc_replacer.SetEvictable(3, true);
  // 现在 [][(6,f1),(7,f2),(2,f3)]![][(5,_),(4,_),(1,_)] p=3

  // 驱逐页面6
  ASSERT_EQ(1, arc_replacer.Evict());
  // 现在 [(6,_)][(7,f2),(2,f3)]![][(5,_),(4,_),(1,_)] p=3
  // 访问被移除的页面3
  // 那么这是情况4A，幽灵页面6将被移除
  arc_replacer.RecordAccess(1, 3);
  arc_replacer.SetEvictable(1, true);
  // 现在 [][(7,f2),(2,f3),(3,f1)]![][(5,_),(4,_),(1,_)] p=3

  // 最后我们驱逐所有页面，看看顺序是否正确，
  // 注意目标大小是3
  ASSERT_EQ(2, arc_replacer.Evict());
  ASSERT_EQ(3, arc_replacer.Evict());
  ASSERT_EQ(1, arc_replacer.Evict());
}

// 欢迎编写更多测试！

}  // namespace bustub