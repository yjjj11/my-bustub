//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_utils.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DeleteTestNoIterator) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }
  std::cout << "we are here\n";
  tree.Draw(bpm, "tree.dot");
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  std::cout << "we are here\n";
  // std::cout<<"我们到这里了\n";
  std::cout << "删除1\n";
  index_key.SetFromInteger(1);
  tree.Remove(index_key);
  tree.Draw(bpm, "tree1.dot");
  std::cout << "删除1ok\n";
  // std::cout<<"删除2\n";
  // index_key.SetFromInteger(2);
  // tree.Remove(index_key);
  // tree.Draw(bpm,"tree2.dot");
  // std::cout<<"删除2ok\n";
  // std::cout<<"删除3\n";
  // index_key.SetFromInteger(3);
  // tree.Remove(index_key);
  // tree.Draw(bpm,"tree3.dot");
  // std::cout<<"删除3ok\n";
  // std::cout<<"删除4\n";
  // index_key.SetFromInteger(4);
  // tree.Remove(index_key);
  // tree.Draw(bpm,"tree4.dot");
  // std::cout<<"删除4ok\n";
  // std::cout<<"删除5\n";
  // index_key.SetFromInteger(5);
  // tree.Remove(index_key);
  // tree.Draw(bpm,"tree5.dot");
  // std::cout<<"删除5ok\n";
  delete bpm;
  return;
  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key);
  }
  std::cout << "we are here\n";
  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      ++size;
    }
  }
  EXPECT_EQ(size, 1);

  // Remove the remaining key
  index_key.SetFromInteger(2);
  tree.Remove(index_key);
  auto root_page_id = tree.GetRootPageId();
  ASSERT_EQ(root_page_id, INVALID_PAGE_ID);
  std::cout << "we are here\n";
  delete bpm;
}

TEST(BPlusTreeTests, OptimisticDeleteTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 4, 3);
  GenericKey<8> index_key;
  RID rid;

  size_t num_keys = 25;
  for (size_t i = 0; i < num_keys; i++) {
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);
    index_key.SetFromInteger(i);
    tree.Insert(index_key, rid);
  }

  size_t to_delete = num_keys + 1;
  auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>>(tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    if ((*leaf)->GetSize() > (*leaf)->GetMinSize()) {
      to_delete = (*leaf)->KeyAt(0).GetAsInteger();
    }
    ++leaf;
  }

  auto base_reads = tree.bpm_->GetReads();
  auto base_writes = tree.bpm_->GetWrites();

  index_key.SetFromInteger(to_delete);
  tree.Remove(index_key);
  std::cout << "we are here\n";
  auto new_reads = tree.bpm_->GetReads();
  auto new_writes = tree.bpm_->GetWrites();

  EXPECT_GT(new_reads - base_reads, 0);
  EXPECT_EQ(new_writes - base_writes, 1);

  delete bpm;
}

TEST(BPlusTreeTests, SequentialEdgeMixTest) {  // NOLINT
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  for (int leaf_max_size = 2; leaf_max_size <= 5; leaf_max_size++) {
    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2> tree("foo_pk", page_id, bpm, comparator, leaf_max_size, 3);
    GenericKey<8> index_key;
    RID rid;
    std::cout << "--------------------------------------------------------当前leaf_max_size=" << leaf_max_size
              << "--------------------------\n";
    std::vector<int64_t> keys = {1, 5, 15, 20, 25, 2, -1, -2, 6, 14, 4};
    std::vector<int64_t> inserted = {};
    std::vector<int64_t> deleted = {};
    for (auto key : keys) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree.Insert(index_key, rid);
      inserted.push_back(key);
      auto res = TreeValuesMatch<GenericKey<8>, RID, GenericComparator<8>, 2>(tree, inserted, deleted);
      ASSERT_TRUE(res);
    }
    tree.Draw(bpm, "treeall.dot");
    std::cout << "------------------------------------------------------------------------全部插入完成-----------------"
                 "---------\n";
    index_key.SetFromInteger(1);
    tree.Remove(index_key);
    deleted.push_back(1);
    inserted.erase(std::find(inserted.begin(), inserted.end(), 1));
    auto res = TreeValuesMatch<GenericKey<8>, RID, GenericComparator<8>, 2>(tree, inserted, deleted);
    ASSERT_TRUE(res);
    tree.Draw(bpm, "treeremove1.dot");
    std::cout << "------------------------------------------------------------------------删除1完成--------------------"
                 "------\n";
    index_key.SetFromInteger(3);
    rid.Set(3, 3);
    tree.Insert(index_key, rid);
    inserted.push_back(3);
    res = TreeValuesMatch<GenericKey<8>, RID, GenericComparator<8>, 2>(tree, inserted, deleted);
    ASSERT_TRUE(res);
    tree.Draw(bpm, "treeinsert3.dot");
    std::cout << "------------------------------------------------------------------------插入3完成--------------------"
                 "------\n";
    keys = {4, 14, 6, 2, 15, -2, -1, 3, 5, 25, 20};
    for (auto key : keys) {
      std::cout << "当前要删除" << key << "\n";
      index_key.SetFromInteger(key);
      tree.Remove(index_key);
      deleted.push_back(key);
      inserted.erase(std::find(inserted.begin(), inserted.end(), key));
      if (key == 2) tree.Draw(bpm, "tree2.dot");
      res = TreeValuesMatch<GenericKey<8>, RID, GenericComparator<8>, 2>(tree, inserted, deleted);
      ASSERT_TRUE(res);
      if (key == 4) tree.Draw(bpm, "tree4.dot");
      if (key == 14) tree.Draw(bpm, "tree14.dot");
      if (key == 6) tree.Draw(bpm, "tree6.dot");
    }
    std::cout << "-----------------------------------------------------------------------批量删除完成完成--------------"
                 "------------\n";
  }

  delete bpm;
}
}  // namespace bustub
