//===----------------------------------------------------------------------===//
//
//                         BusTub（巴士桶）
//
// b_plus_tree.cpp
//
// 标识：src/storage/index/b_plus_tree.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库组
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {  // 这个header_page_id_存的是当前创建的索引的元数据存放在哪一页
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;  // 初始化时，还未插入任何节点，表明树还未创建，因此根节点的page_id为空
}

/**
 * @brief 判断当前 B+ 树是否为空的辅助函数
 * @return 如果此 B+ 树没有键和值，则返回 true。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);  // 读锁（只读操作）
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;  // 比较根页ID是否无效
}

/*****************************************************************************
 * 查找
 *****************************************************************************/
/**
 * @brief 返回与输入键相关联的唯一值
 *
 * 此方法用于点查询
 *
 * @param key 输入键
 * @param[out] result 存储与输入键相关联的唯一值的向量（如果值存在）
 * @return ：true 表示键存在
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  page_id_t root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }

  page_id_t current_page_id = root_page_id;
  ReadPageGuard parent_guard = bpm_->ReadPage(current_page_id);
  auto parent_page = parent_guard.As<BPlusTreePage>();  // 先获取父锁

  while (true) {
    if (parent_page->IsLeafPage()) {
      auto leaf_page = parent_guard.As<LeafPage>();
      int index = leaf_page->FindFirstGreaterOrEqual(key, comparator_);
      if (index == leaf_page->GetSize()) {
        parent_guard.Drop();
        return false;
      } else if (comparator_(leaf_page->KeyAt(index), key) == 0) {  // 说明找到了
        ValueType value = leaf_page->ValueAt(index);
        result->push_back(value);
        parent_guard.Drop();  // 释放叶子页锁
        return true;
      } else {
        parent_guard.Drop();
        return false;
      }
    }
    auto internal_page = parent_guard.As<InternalPage>();
    current_page_id = internal_page->FindPage(key, comparator_);
    auto child_guard = bpm_->ReadPage(current_page_id);  // 获取子锁

    // 获取成功后释放父锁
    parent_guard.Drop();
    parent_guard = std::move(child_guard);
    parent_page = parent_guard.As<BPlusTreePage>();
  }
}

/*****************************************************************************
 * 插入
 *****************************************************************************/
/**
 * @brief 向 B+ 树插入常量键值对
 *
 * 如果当前树为空，则启动新树，更新根页 ID 并插入条目；否则，插入到叶子页中。
 *
 * @param key 要插入的键
 * @param value 与键相关联的值
 * @return ：由于仅支持唯一键，如果用户尝试插入重复键，则返回 false；否则返回 true。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  page_id_t current_page_id = GetRootPageId();

  if (current_page_id == INVALID_PAGE_ID) {
    return Insert_write(key, value);
  }

  ctx.read_set_.push_back(bpm_->ReadPage(current_page_id));
  auto bplus_page = ctx.read_set_.back().As<BPlusTreePage>();
  while (true) {
    if (bplus_page->IsLeafPage()) {
      // 插入到叶子页
      while (!ctx.read_set_.empty()) {
        ctx.read_set_.pop_front();  // 释放所有剩余读锁
      }
      if (bplus_page->GetSize() >= bplus_page->GetMaxSize()) {
        return Insert_write(key, value);
      }
      ctx.write_set_.push_back(bpm_->WritePage(current_page_id));
      int ret = InsertIntoLeaf(ctx, current_page_id, key, value);
      ctx.Drop_write_latch();
      return ret;
    }

    // 内部页：继续向下遍历（当前页锁已存入write_set_）
    // 这个时候可以检查一下，因为我是插入操作，当我的内部页节点size+1后不会
    // 溢出，那么可以确定这次操作不会和修改我上方的父节点，可以释放锁；
    auto internal = ctx.read_set_.back().As<InternalPage>();
    // std::cout << "当前内部页的大小为 " << internal->GetSize() << "\n";
    current_page_id = internal->FindPage(key, comparator_);

    ctx.read_set_.push_back(bpm_->ReadPage(current_page_id));  // 获取子结点锁
    ctx.read_set_.pop_front();                                 // 释放父锁

    bplus_page = ctx.read_set_.back().As<BPlusTreePage>();  // 子锁变成父锁
  }
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert_write(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    // std::cout << "//树还没有被创建的时候，根为空时，创建页子结点为根\n";

    page_id_t new_root_id = bpm_->NewPage();
    if (new_root_id == INVALID_PAGE_ID) {
      std::cout << "//当前无法驱逐内存帧，并无法创建新页\n";
      ctx.Drop_head();
      return false;
    }

    WritePageGuard root_guard = bpm_->WritePage(new_root_id);
    auto root_leaf = root_guard.AsMut<LeafPage>();
    root_leaf->Init(leaf_max_size_);

    // 插入第一个 键值对（唯一键，无需检查重复）s
    root_leaf->Insert(key, value, comparator_);
    header_page->root_page_id_ = new_root_id;  // 将根id更新为新页id
    ctx.Drop_head();
    // std::cout << "当前新页大小为1\n";
    return true;
  }
  ctx.Drop_head();
  page_id_t current_page_id = GetRootPageId();
  if (current_page_id != ctx.root_page_id_) {
    Insert(key, value);
  }
  ctx.write_set_.push_back(bpm_->WritePage(current_page_id));
  auto bplus_page = ctx.write_set_.back().As<BPlusTreePage>();

  while (true) {
    if (bplus_page->IsLeafPage()) {
      // 插入到叶子页
      bool ret = InsertIntoLeaf(ctx, current_page_id, key, value);
      ctx.Drop_write_latch();
      return ret;
    }

    // 内部页：继续向下遍历（当前页锁已存入write_set_）
    // 这个时候可以检查一下，因为我是插入操作，当我的内部页节点size+1后不会
    // 溢出，那么可以确定这次操作不会和修改我上方的父节点，可以释放锁；
    auto internal = ctx.write_set_.back().As<InternalPage>();
    // std::cout << "当前内部页的大小为 " << internal->GetSize() << "\n";
    current_page_id = internal->FindPage(key, comparator_);
    auto child_guard = bpm_->WritePage(current_page_id);  // 获取子结点的锁

    if (internal->GetSize() < internal->GetMaxSize())
      ctx.keep_last_write_latch();  // 就算新增节点也不会上溢，尝试释放前面无关的锁

    ctx.write_set_.push_back(std::move(child_guard));
    bplus_page = ctx.write_set_.back().As<BPlusTreePage>();  // 子锁变成父锁
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::CreateNewRoot(page_id_t left_page_id, const KeyType &split_key, page_id_t right_page_id,
                                   Context &ctx) {
  WritePageGuard header_guard = bpm_->WritePage(header_page_id_);

  page_id_t new_root_id = bpm_->NewPage();
  if (new_root_id == INVALID_PAGE_ID) {
    std::cout << "//当前无法驱逐内存帧，并无法创建新根页\n";
    return false;
  }

  WritePageGuard new_root_guard = bpm_->WritePage(new_root_id);
  auto new_root = new_root_guard.AsMut<InternalPage>();
  new_root->Init(internal_max_size_);
  std::cout << "初始化新内部页作根页成功,新根页id为" << new_root_id << "\n";
  // 初始化新根：左侧子节点+分裂键+右侧子节点
  new_root->SetSize(2);  // 2个子节点对应1个有效键
  new_root->SetValueAt(0, left_page_id);
  new_root->SetValueAt(1, right_page_id);
  new_root->SetKeyAt(1, split_key);
  std::cout << "新内部页大小为2\n";
  std::cout << "左页右页分别是" << left_page_id << "   " << right_page_id << "\n";
  // 更新头部页的根ID

  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = new_root_id;
  header_guard.Drop();
  new_root_guard.Drop();
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(Context &ctx, page_id_t leaf_page_id, const KeyType &key, const ValueType &value)
    -> bool {
  auto leaf = ctx.write_set_.back().AsMut<LeafPage>();

  // 检查重复键
  int insert_pos = leaf->FindFirstGreaterOrEqual(key, comparator_);
  if (insert_pos < leaf->GetSize() && comparator_(leaf->KeyAt(insert_pos), key) == 0) {
    std::cout << "/ 重复键，插入失败";
    return false;
  }

  // 节点不满：直接插入
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    // std::cout << "叶子节点不满，直接插入\n";
    leaf->Insert(key, value, comparator_);
    // std::cout << "当前页大小为" << leaf->GetSize() << "\n";
    return true;
  }

  std::cout << "叶子节点满了，需要分裂上溢\n";
  // 节点已满：用临时容器存储所有数据（旧数据+新数据）
  std::vector<std::pair<KeyType, ValueType>> all_data;
  // 复制旧数据
  for (int i = 0; i < leaf->GetSize(); ++i) {
    all_data.emplace_back(leaf->KeyAt(i), leaf->ValueAt(i));
  }
  // 插入新数据并保持有序
  all_data.emplace(all_data.begin() + insert_pos, key, value);

  // 计算分裂点（均衡拆分）
  int split_idx = (all_data.size() + 1) / 2;      // 前半部分略多
  KeyType split_key = all_data[split_idx].first;  // 新页第一个键作为分裂键
  std::cout << "这次页分裂选举出来的分裂建是" << split_key << "\n";
  // 创建新叶子页
  page_id_t new_leaf_id = bpm_->NewPage();
  if (new_leaf_id == INVALID_PAGE_ID) {
    std::cout << "//当前无法驱逐内存帧，并无法创建新页\n";
    return false;
  }
  std::cout << "新创建的叶子页id为" << new_leaf_id << "\n";
  WritePageGuard new_leaf_guard = bpm_->WritePage(new_leaf_id);
  auto new_leaf = new_leaf_guard.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);

  // 填充旧页（前半部分）
  leaf->Insert_Set_old(split_idx, all_data);
  // 填充新页
  new_leaf->Insert_Set_new(split_idx, all_data);
  std::cout << "旧页的大小" << leaf->GetSize() << "\n";
  std::cout << "新页的大小" << new_leaf->GetSize() << "\n";
  // 更新叶子页链表和父节点
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_id);

  // 向上传递分裂，更新父节点
  if (leaf_page_id == GetRootPageId()) {  // 叶子页是根，创建新根
    std::cout << "// 叶子页是根，创建新根\n";
    return CreateNewRoot(leaf_page_id, split_key, new_leaf_id, ctx);
  } else {
    // 递归更新父内部页
    ctx.write_set_.pop_back();
    assert(!ctx.write_set_.empty());
    page_id_t inter_page = ctx.write_set_.back().GetPageId();
    return InsertIntoInternal(ctx, inter_page, split_key, new_leaf_id);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoInternal(Context &ctx, page_id_t internal_page_id, const KeyType &key,
                                        page_id_t child_page_id) -> bool {
  auto internal = ctx.write_set_.back().AsMut<InternalPage>();
  std::cout << "进入insertintointernal,当前的内部页id是" << internal_page_id << "\n";
  std::cout << "当前内部页大小" << internal->GetSize() << "\n";
  int insert_pos = internal->FindInsertPos(key, comparator_);
  std::cout << "FindInsertPos返回插入位置: " << insert_pos << "\n";
  if (insert_pos == -1) return false;

  // 节点不满：直接插入
  if (internal->GetSize() < internal->GetMaxSize()) {
    // 移动数据
    std::cout << "内部页节点不满，直接插入\n";
    internal->Insert(key, child_page_id, insert_pos);
    return true;
  }

  std::cout << "内部也满了，需要分裂\n";
  // 节点已满：用临时容器存储所有数据（旧数据+新数据）
  std::vector<std::pair<KeyType, page_id_t>> all_data;
  // 复制旧数据（键+子节点ID）
  all_data.emplace_back(KeyType{}, internal->ValueAt(0));  // 第一个子节点（无前置键）
  for (int i = 1; i < internal->GetSize(); ++i) {
    all_data.emplace_back(internal->KeyAt(i), internal->ValueAt(i));
  }
  // 插入新数据并保持有序
  all_data.emplace(all_data.begin() + insert_pos, key, child_page_id);
  for (auto &data : all_data) {
    std::cout << data.first << "  ";
  }
  std::cout << "\nall_data.size()=" << all_data.size() << "\n";
  // 计算分裂点（均衡拆分）
  int split_idx = (all_data.size() + 1) / 2;      // 子节点分裂点
  KeyType split_key = all_data[split_idx].first;  // 分裂键（新页第一个有效键）
  std::cout << "内部页分裂出的分裂建" << split_key << "\n";
  // 创建新内部页
  page_id_t new_internal_id = bpm_->NewPage();
  if (new_internal_id == INVALID_PAGE_ID) {
    std::cout << "//当前无法驱逐内存帧，并无法创建新内部页In InsertIntoInternal\n";
    return false;
  }
  std::cout << "新内部页的id为" << new_internal_id << "\n";
  WritePageGuard new_internal_guard = bpm_->WritePage(new_internal_id);
  auto new_internal = new_internal_guard.AsMut<InternalPage>();
  new_internal->Init(internal_max_size_);

  // 填充旧页（前半部分）
  std::cout << "旧页id=" << internal_page_id << " ,其中键值对为：\n";
  internal->Insert_Set_old(split_idx, all_data);

  // 填充新页（后半部分）
  std::cout << "新页id=" << new_internal_id << " ,其中键值对为：\n";
  new_internal->Insert_Set_new(split_idx, all_data);

  // 向上传递分裂，直至根节点
  if (internal_page_id == GetRootPageId()) {
    std::cout << "内部页是根，创建新根\n";
    return CreateNewRoot(internal_page_id, split_key, new_internal_id, ctx);
  } else {
    // 递归更新父内部页
    ctx.write_set_.pop_back();
    assert(!ctx.write_set_.empty());
    auto father_id = ctx.write_set_.back().GetPageId();
    return InsertIntoInternal(ctx, father_id, split_key, new_internal_id);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsLeafUnderflow(LeafPage *leaf) const {
  return leaf->GetSize() < (leaf->GetMaxSize() + 1) / 2;  // 向上取整，确保最小容量
}

// 辅助函数：检查内部页是否下溢（最小容量为 (max_size_ - 1) / 2，因内部页键数=子节点数-1）
FULL_INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsInternalUnderflow(InternalPage *internal) const {
  std::cout << "进入了IsInternalUnderflow\n";
  std::cout << "当前页面大小为" << internal->GetSize() << "\n";
  std::cout << "最小下限大小为" << (internal->GetMaxSize() + 1) / 2 << "\n";
  bool ret = internal->GetSize() < (internal->GetMaxSize() + 1) / 2;  // 内部页最小子节点数
  std::cout << ret << "\n";
  return ret;
}

// 首先取出存索引元数据的页id并观察其中的root_page_id_是否为invalid_page;
// 如果是，要重新开始创建根节点的新页，并更新元数据的root_page_id_为新页的id
// 否则，就一直查找到页节点相应的位置并插入

/*****************************************************************************
 * 删除
 *****************************************************************************/
/**
 * @brief 删除与输入键相关联的键值对
 * 如果当前树为空，立即返回。
 * 如果非空，用户需要先找到作为删除目标的叶子页，然后从叶子页中删除条目。
 * 记住必要时需要处理重分配或合并操作。
 *
 * @param key 输入键
 */
// 注意：保留原有模板宏定义，此处省略 FULL_INDEX_TEMPLATE_ARGUMENTS 重复定义
// 你需要在原有代码的对应位置插入打印语句

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  std::cout << "进入remove\n";
  // Context 实例的声明。
  Context ctx;
  // 步骤1：获取头部页，检查树是否为空
  page_id_t root_page_id = GetRootPageId();
  ctx.root_page_id_ = root_page_id;
  if (root_page_id == INVALID_PAGE_ID) return;
  std::cout << "get rootpage_id: " << root_page_id << "\n";
  // 步骤2：从根向下遍历，定位叶子页（全程持有写锁）
  page_id_t current_page_id = root_page_id;
  ctx.read_set_.push_back(bpm_->ReadPage(current_page_id));
  std::cout << "新持有读锁，页面ID: " << current_page_id << "\n";  // 新增打印
  auto bplus_page = ctx.read_set_.back().As<BPlusTreePage>();
  std::cout << "get bplus_page\n";
  while (true) {
    if (bplus_page->IsLeafPage()) {
      ctx.read_set_.pop_back();
      if (bplus_page->GetSize() <= (leaf_max_size_ + 1) / 2) {
        std::cout << "删除后会发生连锁操作\n";
        return Remove_write(key);
      }
      ctx.write_set_.push_back(bpm_->WritePage(current_page_id));
      std::cout << "新持有写锁，页面ID: " << current_page_id << "\n";  // 新增打印
      RemoveFromLeaf(ctx, current_page_id, key);
      ctx.Drop_write_latch();
      return;
    }

    auto internal = ctx.read_set_.back().As<InternalPage>();
    current_page_id = internal->FindPage(key, comparator_);
    std::cout << "找到的页id是" << current_page_id << "\n";
    ctx.read_set_.push_back(bpm_->ReadPage(current_page_id));        // 获取子锁
    std::cout << "新持有读锁，页面ID: " << current_page_id << "\n";  // 新增打印
    ctx.read_set_.pop_front();                                       // 释放父锁
    bplus_page = ctx.read_set_.back().As<BPlusTreePage>();
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove_write(const KeyType &key) {
  std::cout << "进入remove_write\n";
  // Context 实例的声明。
  Context ctx;
  // 步骤1：获取头部页，检查树是否为空
  page_id_t root_page_id = GetRootPageId();
  ctx.root_page_id_ = root_page_id;
  if (root_page_id == INVALID_PAGE_ID) return;

  // 步骤2：从根向下遍历，定位叶子页（全程持有写锁）
  page_id_t current_page_id = root_page_id;

  ctx.write_set_.push_back(bpm_->WritePage(current_page_id));
  std::cout << "新持有写锁，页面ID: " << current_page_id << "\n";  // 新增打印
  auto bplus_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  while (true) {
    if (bplus_page->IsLeafPage()) {
      RemoveFromLeaf(ctx, current_page_id, key);
      ctx.Drop_write_latch();
      return;
    }

    auto internal = ctx.write_set_.back().As<InternalPage>();

    current_page_id = internal->FindPage(key, comparator_);
    auto child = bpm_->WritePage(current_page_id);
    std::cout << "新持有写锁，页面ID: " << current_page_id << "\n";  // 新增打印
    if (internal->GetSize() > (internal_max_size_ + 1) / 2) {
      // 就算下方有节点下溢，最终本节点仍然满足半满，因此不用担心前面的地方发生修改，释放路径上的锁
      ctx.keep_last_write_latch();
    }
    ctx.write_set_.push_back(std::move(child));
    bplus_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    std::cout << "找到的页id是" << current_page_id << "\n";
  }
}

// 查找叶子页的兄弟页及父节点信息
FULL_INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::FindLeafSibling(Context &ctx, page_id_t current_page_id, page_id_t *sibling_page_id_left,
                                     page_id_t *sibling_page_id_right, int *current_idx, int *sibling_idx_left,
                                     int *sibling_idx_right, InternalPage **parent_internal) {
  int size = ctx.write_set_.size();
  if (size == 1) {
    return false;  // 根叶子页无兄弟
  }

  // 获取父节点
  auto &parent_guard = ctx.write_set_[size - 2];
  *parent_internal = parent_guard.AsMut<InternalPage>();
  *current_idx = (*parent_internal)->ValueIndex(current_page_id);
  if (*current_idx == -1) {
    return false;  // 未找到当前页在父节点中的位置
  }
  bool has_sibling = false;
  // 优先找左兄弟，再找右兄弟
  if (*current_idx > 0) {
    *sibling_idx_left = *current_idx - 1;
    *sibling_page_id_left = (*parent_internal)->ValueAt(*sibling_idx_left);
    has_sibling = true;
  }
  if (*current_idx < (*parent_internal)->GetSize() - 1) {
    *sibling_idx_right = *current_idx + 1;
    *sibling_page_id_right = (*parent_internal)->ValueAt(*sibling_idx_right);
    has_sibling = true;
  }
  return has_sibling;
}

// 叶子页重分配（从兄弟页借键）
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeLeaf(Context &ctx, LeafPage *current_leaf, LeafPage *sibling_leaf, int current_idx,
                                      int sibling_idx, InternalPage *parent) {
  if (sibling_idx < current_idx) {
    std::cout << "从左兄弟借最后一个键\n";
    KeyType borrow_key = sibling_leaf->KeyAt(sibling_leaf->GetSize() - 1);
    ValueType borrow_val = sibling_leaf->ValueAt(sibling_leaf->GetSize() - 1);
    sibling_leaf->RemoveAt(sibling_leaf->GetSize() - 1);
    std::cout << "借的键为" << borrow_key << "\n";
    std::cout << "插入到当前页头部\n";
    current_leaf->Insert(borrow_key, borrow_val, comparator_);
    parent->SetKeyAt(current_idx, borrow_key);
  } else {
    std::cout << "从右兄弟借第一个键\n";
    KeyType borrow_key = sibling_leaf->KeyAt(0);
    ValueType borrow_val = sibling_leaf->ValueAt(0);
    sibling_leaf->RemoveAt(0);
    std::cout << "借的键为" << borrow_key << "\n";
    //
    std::cout << "插入到当前页尾部\n";
    current_leaf->Insert(borrow_key, borrow_val, comparator_);
    parent->SetKeyAt(sibling_idx, sibling_leaf->KeyAt(0));
  }
}

// 叶子页合并（当前页合并到兄弟页）
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeaf(Context &ctx, LeafPage *current_leaf, LeafPage *sibling_leaf, int current_idx,
                               int sibling_idx, InternalPage *parent, page_id_t current_page_id) {
  std::cout << "进入MergeLeaf\n";
  if (sibling_idx < current_idx) {
    // 合并到左兄弟
    std::cout << "合并到左兄弟\n";
    for (int i = 0; i < current_leaf->GetSize(); ++i) {
      sibling_leaf->InsertAt(sibling_leaf->GetSize(), current_leaf->KeyAt(i), current_leaf->ValueAt(i));
    }
    // 删除父节点中当前页对应的索引项
    sibling_leaf->SetNextPageId(current_leaf->GetNextPageId());
    parent->RemoveAt(current_idx);
    bpm_->DeletePage(current_page_id);
  } else {
    std::cout << "合并到本节点\n";
    for (int i = 0; i < sibling_leaf->GetSize(); ++i) {
      current_leaf->InsertAt(current_leaf->GetSize(), sibling_leaf->KeyAt(i), sibling_leaf->ValueAt(i));
    }
    std::cout << "当前页面内的键有:\n";
    for (int i = 0; i < current_leaf->GetSize(); ++i) {
      std::cout << current_leaf->KeyAt(i) << " ";
    }
    std::cout << "\n";
    auto sibling_page_id = current_leaf->GetNextPageId();
    current_leaf->SetNextPageId(sibling_leaf->GetNextPageId());
    sibling_leaf->SetNextPageId(INVALID_PAGE_ID);

    parent->RemoveAt(sibling_idx);
    bpm_->DeletePage(sibling_page_id);
  }
}

// 处理叶子页下溢（触发父节点处理）
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleLeafUnderflow(Context &ctx, page_id_t current_page_id) {
  // 根叶子页特殊处理：下溢无需处理（可为空）
  if (current_page_id == ctx.root_page_id_) {
    auto current_leaf = ctx.write_set_.back().AsMut<LeafPage>();
    if (current_leaf->GetSize() == 0) {
      auto header_guard = bpm_->WritePage(header_page_id_);
      std::cout << "新持有写锁，页面ID: " << header_page_id_ << "\n";  // 新增打印
      auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = INVALID_PAGE_ID;  // 整颗树被删除了
    }
    return;
  }
  auto current_leaf = ctx.write_set_.back().AsMut<LeafPage>();
  // 查找兄弟页和父节点
  page_id_t sibling_page_id_left = INVALID_PAGE_ID;  // 兄弟页
  page_id_t sibling_page_id_right = INVALID_PAGE_ID;
  int current_idx = -1;
  int sibling_idx_left = -1;
  int sibling_idx_right = -1;
  InternalPage *parent_internal = nullptr;
  if (!FindLeafSibling(ctx, current_page_id, &sibling_page_id_left, &sibling_page_id_right, &current_idx,
                       &sibling_idx_left, &sibling_idx_right, &parent_internal)) {
    ctx.write_set_.pop_back();
    return;
  }
  int sibli_size = 0;
  LeafPage *sibling_leaf_left = nullptr;
  LeafPage *sibling_leaf_right = nullptr;
  // 获取兄弟页锁
  if (sibling_page_id_left != INVALID_PAGE_ID) {
    std::cout << "有左兄弟页，页id=" << sibling_page_id_left << "\n";
    ctx.write_set_.push_back(bpm_->WritePage(sibling_page_id_left));
    std::cout << "新持有写锁，页面ID: " << sibling_page_id_left << "\n";  // 新增打印
    auto &sibling_guard = ctx.write_set_.back();
    sibling_leaf_left = sibling_guard.AsMut<LeafPage>();
    sibli_size++;
  }
  if (sibling_page_id_right != INVALID_PAGE_ID) {
    std::cout << "有右兄弟页，页id=" << sibling_page_id_right << "\n";
    ctx.write_set_.push_back(bpm_->WritePage(sibling_page_id_right));
    std::cout << "新持有写锁，页面ID: " << sibling_page_id_right << "\n";  // 新增打印
    sibling_leaf_right = ctx.write_set_.back().AsMut<LeafPage>();
    sibli_size++;
  }

  bool handled = false;
  // 尝试重分配，失败则合并
  if (sibling_leaf_left != nullptr && sibling_leaf_left->GetSize() > (sibling_leaf_left->GetMaxSize() + 1) / 2) {
    std::cout << "左兄弟可以借键\n";
    RedistributeLeaf(ctx, current_leaf, sibling_leaf_left, current_idx, sibling_idx_left, parent_internal);
    handled = true;
  } else if (sibling_leaf_right != nullptr &&
             sibling_leaf_right->GetSize() > (sibling_leaf_right->GetMaxSize() + 1) / 2) {
    std::cout << "右兄弟可以借键\n";
    RedistributeLeaf(ctx, current_leaf, sibling_leaf_right, current_idx, sibling_idx_right, parent_internal);
    handled = true;
  }
  if (handled) {
    std::cout << "借键完成，释放兄弟页的锁\n";
    while (sibli_size > 0) {
      ctx.write_set_.pop_back();
      sibli_size--;
    }
    ctx.write_set_.pop_back();
  } else {
    if (sibling_page_id_left != INVALID_PAGE_ID) {
      std::cout << "左兄弟可以合并\n";
      MergeLeaf(ctx, current_leaf, sibling_leaf_left, current_idx, sibling_idx_left, parent_internal, current_page_id);
    } else {
      std::cout << "右兄弟可以合并\n";
      MergeLeaf(ctx, current_leaf, sibling_leaf_right, current_idx, sibling_idx_right, parent_internal,
                current_page_id);
    }
    while (sibli_size > 0) {
      ctx.write_set_.pop_back();
      sibli_size--;
    }
    // 合并后父节点可能下溢，递归处理
    ctx.write_set_.pop_back();
    assert(!ctx.write_set_.empty());
    HandleInternalUnderflow(ctx, ctx.write_set_.back().GetPageId());
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::RemoveFromLeaf(Context &ctx, page_id_t leaf_page_id, const KeyType &key) {
  std::cout << "进入RemoveFromLeaf\n";
  auto leaf = ctx.write_set_.back().AsMut<LeafPage>();

  // 查找待删除键的位置
  int delete_pos = leaf->FindFirstGreaterOrEqual(key, comparator_);
  if (delete_pos >= leaf->GetSize() || comparator_(leaf->KeyAt(delete_pos), key) != 0) {
    return false;  // 键不存在
  }
  std::cout << "找到待删除的建的位置为" << delete_pos << "\n";
  // 执行删除
  leaf->RemoveAt(delete_pos);

  // 检查是否下溢，需要处理
  if (IsLeafUnderflow(leaf)) {
    std::cout << "节点需要借键或删除\n";
    HandleLeafUnderflow(ctx, leaf_page_id);
  }
  return true;
}

// 查找内部页的兄弟页及父节点信息（类比 FindLeafSibling）
FULL_INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::FindInternalSibling(Context &ctx, page_id_t current_page_id, page_id_t *sibling_page_id_left,
                                         page_id_t *sibling_page_id_right, int *current_idx, int *sibling_idx_left,
                                         int *sibling_idx_right, InternalPage **parent_internal) {
  std::cout << "进入了内部页找左右兄弟\n";
  int size = ctx.write_set_.size();
  if (size == 1) {
    std::cout << "we are here too\n";
    return false;  // 根内部页无兄弟
  }
  std::cout << "we are here\n";
  // 获取父节点（父节点锁在 write_set_ 的倒数第二位）
  auto &parent_guard = ctx.write_set_[size - 2];
  *parent_internal = parent_guard.AsMut<InternalPage>();
  *current_idx = (*parent_internal)->ValueIndex(current_page_id);
  if (*current_idx == -1) {
    return false;  // 父节点中未找到当前页
  }
  std::cout << "在父节点中找到了自己\n";
  // 初始化兄弟页信息
  *sibling_page_id_left = INVALID_PAGE_ID;
  *sibling_page_id_right = INVALID_PAGE_ID;
  *sibling_idx_left = -1;
  *sibling_idx_right = -1;
  bool has_sibling = false;

  // 查找左兄弟
  if (*current_idx > 0) {
    *sibling_idx_left = *current_idx - 1;
    *sibling_page_id_left = (*parent_internal)->ValueAt(*sibling_idx_left);
    has_sibling = true;
    std::cout << "有左兄弟\n";
  }
  // 查找右兄弟
  if (*current_idx < (*parent_internal)->GetSize() - 1) {
    *sibling_idx_right = *current_idx + 1;
    *sibling_page_id_right = (*parent_internal)->ValueAt(*sibling_idx_right);
    has_sibling = true;
    std::cout << "有右兄弟\n";
  }

  return has_sibling;
}
// 内部页重分配（类比 RedistributeLeaf）
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeInternal(Context &ctx, InternalPage *current_internal, InternalPage *sibling_internal,
                                          int current_idx, int sibling_idx, InternalPage *parent) {
  if (sibling_idx < current_idx) {
    int borrow_pos = sibling_internal->GetSize() - 1;
    std::cout << "和左兄弟合并\n";
    KeyType borrow_key = sibling_internal->KeyAt(borrow_pos);
    std::cout << "要插入到父节点的key=" << borrow_key << "\n";
    page_id_t borrow_sub_id = sibling_internal->ValueAt(borrow_pos);
    // 左兄弟删除该子节点
    sibling_internal->RemoveAt(borrow_pos);
    KeyType parent_key = parent->KeyAt(current_idx);
    current_internal->InsertAt_head(parent_key, borrow_sub_id);

    // 更新父节点的分隔键（左兄弟的最后一个键成为新分隔键）
    if (sibling_internal->GetSize() > 1) {
      parent->SetKeyAt(current_idx, borrow_key);
    }
  } else {
    std::cout << "和右兄弟合并\n";
    // 右兄弟的第一个子节点的分隔键和页ID
    KeyType borrow_key = sibling_internal->KeyAt(1);
    std::cout << "要插入到父节点的key=" << borrow_key << "\n";
    page_id_t borrow_sub_id = sibling_internal->ValueAt(0);
    sibling_internal->RemoveAt_head();

    // 当前页插入到尾部（使用父节点的分隔键作为当前页的新分隔键）
    KeyType parent_key = parent->KeyAt(sibling_idx);
    current_internal->InsertAt(current_internal->GetSize(), parent_key, borrow_sub_id);

    // 更新父节点的分隔键（右兄弟的第一个键成为新分隔键）
    if (sibling_internal->GetSize() > 1) {
      parent->SetKeyAt(sibling_idx, borrow_key);
    }
  }
}

// 内部页合并（类比 MergeLeaf）
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeInternal(Context &ctx, InternalPage *current_internal, InternalPage *sibling_internal,
                                   int current_idx, int sibling_idx, InternalPage *parent, page_id_t current_page_id) {
  if (sibling_idx < current_idx) {
    // 1. 插入父节点的分隔键（作为左兄弟与当前页的分隔）
    KeyType split_key = parent->KeyAt(current_idx);
    sibling_internal->InsertAt(sibling_internal->GetSize(), split_key, current_internal->ValueAt(0));
    // 2. 插入当前页的剩余子节点
    for (int i = 1; i < current_internal->GetSize(); ++i) {
      sibling_internal->InsertAt(sibling_internal->GetSize(), current_internal->KeyAt(i), current_internal->ValueAt(i));
    }
    // 3. 删除父节点中当前页的索引
    parent->RemoveAt(current_idx);
    // 4. 释放当前页
    bpm_->DeletePage(current_page_id);
  } else {
    // 合并到右兄弟：右兄弟 → 当前页，删除右兄弟（反转方向，类似叶子页逻辑）
    // 1. 插入父节点的分隔键（作为当前页与右兄弟的分隔）
    KeyType split_key = parent->KeyAt(sibling_idx);
    current_internal->InsertAt(current_internal->GetSize(), split_key, sibling_internal->ValueAt(0));
    // 2. 插入右兄弟的剩余子节点
    for (int i = 1; i < sibling_internal->GetSize(); ++i) {
      current_internal->InsertAt(current_internal->GetSize(), sibling_internal->KeyAt(i), sibling_internal->ValueAt(i));
    }
    // 3. 删除父节点中右兄弟的索引
    parent->RemoveAt(sibling_idx);
    // 4. 释放右兄弟页
    bpm_->DeletePage(ctx.write_set_.back().GetPageId());
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleInternalUnderflow(Context &ctx, page_id_t current_page_id) {
  std::cout << "进入了HandleInternalUnderflow,当前处理的页id是" << current_page_id << "\n";
  std::cout << "整颗树的根页id为" << ctx.root_page_id_ << "\n";
  // 根内部页特殊处理：若只剩1个子节点，升级为新根
  if (current_page_id == ctx.root_page_id_) {
    std::cout << " 根内部页特殊处理:若只剩1个子节点,需要产生下溢,否则无需合并，直接返回\n";
    auto &current_guard = ctx.write_set_.back();
    auto current_internal = current_guard.AsMut<InternalPage>();
    if (current_internal->GetSize() == 1) {
      std::cout << "根内部页已经不存在键了，下溢\n";
      page_id_t new_root_id = current_internal->ValueAt(0);
      // 更新头部页根ID
      WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
      std::cout << "新持有写锁，页面ID: " << header_page_id_ << "\n";  // 新增打印
      header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
      ctx.root_page_id_ = new_root_id;
      std::cout << "新根的页id是" << new_root_id << "\n";
      // 释放旧根页
      bpm_->DeletePage(current_page_id);
    }
    return;
  }
  std::cout << "当前不是根内部页，只考虑借键或者合并\n";
  auto current_internal = ctx.write_set_.back().AsMut<InternalPage>();

  if (!IsInternalUnderflow(current_internal)) return;
  std::cout << "说明当前不是根内部页，考虑借键或者合并\n";
  // 查找兄弟页和父节点
  page_id_t sibling_page_id_left = INVALID_PAGE_ID;
  page_id_t sibling_page_id_right = INVALID_PAGE_ID;
  int current_idx = -1;
  int sibling_idx_left = -1;
  int sibling_idx_right = -1;
  InternalPage *parent_internal = nullptr;
  std::cout << "准备找左右兄弟\n";
  if (!FindInternalSibling(ctx, current_page_id, &sibling_page_id_left, &sibling_page_id_right, &current_idx,
                           &sibling_idx_left, &sibling_idx_right, &parent_internal)) {
    ctx.write_set_.pop_back();  // 释放当前页锁
    return;
  }
  std::cout << "找到了左右兄弟\n";
  // 记录兄弟页锁起始索引，获取兄弟页指针
  size_t sibling_lock_start = ctx.write_set_.size();
  int sibli_size = 0;
  InternalPage *sibling_internal_left = nullptr;
  InternalPage *sibling_internal_right = nullptr;

  if (sibling_page_id_left != INVALID_PAGE_ID) {
    ctx.write_set_.push_back(bpm_->WritePage(sibling_page_id_left));
    std::cout << "新持有写锁，页面ID: " << sibling_page_id_left << "\n";  // 新增打印
    sibling_internal_left = ctx.write_set_.back().AsMut<InternalPage>();
    sibli_size++;
    std::cout << "有左兄弟页\n";
  }
  if (sibling_page_id_right != INVALID_PAGE_ID) {
    ctx.write_set_.push_back(bpm_->WritePage(sibling_page_id_right));
    std::cout << "新持有写锁，页面ID: " << sibling_page_id_right << "\n";  // 新增打印
    sibling_internal_right = ctx.write_set_.back().AsMut<InternalPage>();
    sibli_size++;
    std::cout << "有右兄弟页\n";
  }

  // 尝试重分配，失败则合并
  bool handled = false;
  if (sibling_internal_left != nullptr &&
      sibling_internal_left->GetSize() > (sibling_internal_left->GetMaxSize() + 1) / 2) {
    std::cout << "左兄弟有多余子节点，重分配\n";
    RedistributeInternal(ctx, current_internal, sibling_internal_left, current_idx, sibling_idx_left, parent_internal);
    handled = true;
  } else if (sibling_internal_right != nullptr &&
             sibling_internal_right->GetSize() > (sibling_internal_right->GetMaxSize() + 1) / 2) {
    std::cout << "右兄弟有多余子节点，重分配\n";
    RedistributeInternal(ctx, current_internal, sibling_internal_right, current_idx, sibling_idx_right,
                         parent_internal);
    handled = true;
  }

  if (handled) {
    std::cout << "释放兄弟页锁\n";
    while (ctx.write_set_.size() > sibling_lock_start) {
      ctx.write_set_.pop_back();
    }
    // 释放当前页锁
    ctx.write_set_.pop_back();
    return;
  } else {
    // 兄弟页无多余空间，执行合并
    if (sibling_page_id_left != INVALID_PAGE_ID) {
      std::cout << "与左内部页合并\n";
      MergeInternal(ctx, current_internal, sibling_internal_left, current_idx, sibling_idx_left, parent_internal,
                    current_page_id);
    } else if (sibling_page_id_right != INVALID_PAGE_ID) {
      std::cout << "与右内部页合并\n";
      MergeInternal(ctx, current_internal, sibling_internal_right, current_idx, sibling_idx_right, parent_internal,
                    current_page_id);
    }
    while (ctx.write_set_.size() > sibling_lock_start) {
      ctx.write_set_.pop_back();
    }
    ctx.write_set_.pop_back();
    assert(!ctx.write_set_.empty());
    // 合并后父节点可能下溢，递归处理
    HandleInternalUnderflow(ctx, ctx.write_set_.back().GetPageId());
  }
}
/*****************************************************************************
 * 索引迭代器
 *****************************************************************************/
/**
 * @brief 输入参数为空，首先找到最左侧的叶子页，然后构造索引迭代器
 *
 * 你可能希望在实现任务 #3 时实现此方法。
 *
 * @return ：索引迭代器
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();  // 空树直接返回尾后迭代器
  }
  std::cout << "进入begin()\n";
  page_id_t current_page_id = GetRootPageId();
  ReadPageGuard parent_guard = bpm_->ReadPage(current_page_id);  // 先锁根节点
  auto parent_page = parent_guard.As<BPlusTreePage>();

  // 遍历到最左侧叶子页（蟹型锁逻辑：先锁子，再放父）
  while (true) {
    if (parent_page->IsLeafPage()) {
      std::cout << "当前页的id是 " << current_page_id << "\n";
      // 到达叶子页，返回持有该页读锁的迭代器
      parent_guard.Drop();
      return INDEXITERATOR_TYPE(bpm_.get(), current_page_id, 0);
    }

    // 内部页：获取左子节点（最左侧子树）
    auto internal_page = parent_guard.As<InternalPage>();
    page_id_t child_page_id = internal_page->ValueAt(0);  // 内部页第一个子节点是最左侧

    // 蟹型锁核心：先获取子节点锁，再释放父节点锁
    ReadPageGuard child_guard = bpm_->ReadPage(child_page_id);

    parent_guard.Drop();                    // 释放父节点锁
    parent_guard = std::move(child_guard);  // 子节点锁成为新的父锁
    parent_page = parent_guard.As<BPlusTreePage>();
    current_page_id = child_page_id;
  }
}
/**
 * @brief 输入参数为下限键，首先找到包含输入键的叶子页，然后构造索引迭代器
 * @return ：索引迭代器
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();  // 空树直接返回尾后迭代器
  }

  page_id_t current_page_id = GetRootPageId();
  ReadPageGuard parent_guard = bpm_->ReadPage(current_page_id);  // 先锁根节点
  auto parent_page = parent_guard.As<BPlusTreePage>();

  // 遍历到包含目标键的叶子页（蟹型锁逻辑：先锁子，再放父）
  while (true) {
    if (parent_page->IsLeafPage()) {
      // 叶子页：找到第一个>=key的索引
      auto leaf_page = parent_guard.As<LeafPage>();
      int index = leaf_page->FindFirstGreaterOrEqual(key, comparator_);
      // 返回持有该页读锁的迭代器
      parent_guard.Drop();
      return INDEXITERATOR_TYPE(bpm_.get(), current_page_id, index);
    }

    // 内部页：查找下一层子节点
    auto internal_page = parent_guard.As<InternalPage>();
    page_id_t child_page_id = internal_page->FindPage(key, comparator_);  // 找到目标子节点

    // 蟹型锁核心：先获取子节点锁，再释放父节点锁
    ReadPageGuard child_guard = bpm_->ReadPage(child_page_id);

    parent_guard.Drop();                    // 释放父节点锁
    parent_guard = std::move(child_guard);  // 子节点锁成为新的父锁
    parent_page = parent_guard.As<BPlusTreePage>();
    current_page_id = child_page_id;
  }
}

/**
 * @brief 输入参数为空，构造一个表示叶子节点中键值对末尾的索引迭代器
 * @return ：索引迭代器
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_.get(), INVALID_PAGE_ID, 0); }

/**
 * @return 此树的根节点的页 ID
 *
 * 你可能希望在实现任务 #3 时实现此方法。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
// template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
