#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    SetSize(0);
    SetKeySize(key_size);
    memset(pairs_off, 0, GetMaxSize() * pair_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    if (GetSize() <= 1) {
        return INVALID_PAGE_ID;  // or some default value indicating an error
    }
    // 指针-键对数量 = GetSize()，其中键有效范围是 [1, GetSize()-1]
    int left = 1;
    int right = GetSize() - 1;  // 只在这一区间做二分查找

    // 标准二分查找：找到第一个 keyAt(idx) > key 的 idx
    while (left <= right) {
        int mid = left + ((right - left) >> 1);
        if (KM.CompareKeys(key, KeyAt(mid)) < 0) {
            // key < keyAt(mid)：答案在左半区
            right = mid - 1;
        } else {
            // key >= keyAt(mid)：在右半区继续
            left = mid + 1;
        }
    }

    // 此时 left 是第一个大于搜索 key 的键的位置，
    // 应该下钻到第 (left-1) 个指针
    return ValueAt(left - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    // 1) 作为新根，当前页一开始是空的，调用过 Init() 设置好了 page_type、max_size、key_size…
    // 2) 根内部页至少要保留两个子指针 => size = 2
    SetSize(2);

    // 3) 在槽 0 上放旧根的指针；槽 0 的 key 保持 INVALID
    SetValueAt(0, old_value);

    // 4) 在槽 1 上放新 key + 新页指针
    SetKeyAt(1, new_key);
    SetValueAt(1, new_value);
}


/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    // 1) 先找到 old_value 的位置
    int index = ValueIndex(old_value);
    if (index == -1) {
        return -1;
    }

    // 2) 先把后面的元素都往后挪一个位置
    for (int i = GetSize(); i > index + 1; --i) {
        SetKeyAt(i, KeyAt(i - 1));
        SetValueAt(i, ValueAt(i - 1));
    }

    // 3) 插入新元素
    SetKeyAt(index + 1, new_key);
    SetValueAt(index + 1, new_value);

    // 4) 更新 size
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    // 1) 计算要移动的元素数量
    int size = GetSize();
    // 2) 按 ⌈n/2⌉ 向上取整划分：前半部分留在本页，后半部分搬给 recipient
    int split = (size + 1) / 2;
    int move_cnt = size - split;
    // 3) 从槽 split 开始，把后半部分搬给 recipient
    void *src = PairPtrAt(split);
    recipient->CopyNFrom(src, move_cnt, buffer_pool_manager);
    // 4) 调整本页大小
    SetSize(split);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    // 原来 page 里已有的对数
    int old_size = GetSize();

    // 对每一对执行拷贝 + 父指针更新
    for (int i = 0; i < size; i++) {
        // 目标槽 index
        int dest_idx = old_size + i;
        // 拷贝一对 (key + child_page_id)
        void *dest = PairPtrAt(dest_idx);
        std::memcpy(dest, reinterpret_cast<char *>(src) + i * pair_size, pair_size);

        // 拿到拷贝后的 child_page_id
        page_id_t child = ValueAt(dest_idx);
        // 更新 child 页的 parent_page_id
        Page *child_page = buffer_pool_manager->FetchPage(child);
        reinterpret_cast<BPlusTreePage *>(child_page)->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(child, /*is_dirty=*/true);
    }

    // 更新本页大小
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    // 1) 先把后面的元素都往前挪一个位置
    for (int i = index; i < GetSize() - 1; ++i) {
        SetKeyAt(i, KeyAt(i + 1));
        SetValueAt(i, ValueAt(i + 1));
    }

    // 2) 更新 size
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    // 1) 只有一个元素，直接返回
    page_id_t child = ValueAt(0);
    // 2) 更新 size
    IncreaseSize(-1);
    return child;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    // 1) 把当前页的所有 key-value 对搬给 recipient
    void *src = PairPtrAt(0);  // 从第一个槽开始搬
    int size = GetSize();
    recipient->CopyNFrom(src, size, buffer_pool_manager);

    // 2) 把 middle_key 放到 recipient 的最后一个槽
    recipient->SetKeyAt(recipient->GetSize(), middle_key);

    // 3) 更新 recipient 的 size
    recipient->IncreaseSize(1);

    // 4) 更新当前页的 size 为 0
    SetSize(0);

    // 5） 更新 recipient 中 middle key child 的 parent_page_id
    page_id_t child = recipient->ValueAt(recipient->GetSize() - 1);
    Page *child_page = buffer_pool_manager->FetchPage(child);
    reinterpret_cast<BPlusTreePage *>(child_page)->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child, /*is_dirty=*/true);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    // 1) 把当前页的第一个 key-value 对搬给 recipient
    void *src = PairPtrAt(0);  // 从第一个槽开始搬
    recipient->CopyNFrom(src, 1, buffer_pool_manager);
    // 2) 把 middle_key 放到 recipient 的最后一个槽
    recipient->SetKeyAt(recipient->GetSize(), middle_key);
    // 3) 更新 recipient 的 size
    recipient->IncreaseSize(1);
    // 4) 更新当前页的 size
    IncreaseSize(-1);
    // 5) 把当前页的第一个 key-value 对删除
    Remove(0);
    // 6) 更新 recipient 中 middle key child 的 parent_page_id
    page_id_t child = recipient->ValueAt(recipient->GetSize() - 1);
    Page *child_page = buffer_pool_manager->FetchPage(child);
    reinterpret_cast<BPlusTreePage *>(child_page)->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child, /*is_dirty=*/true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    // 1) 先把 key-value 对拷贝到最后一个槽
    int dest_idx = GetSize();
    SetKeyAt(dest_idx, key);
    SetValueAt(dest_idx, value);

    // 2) 更新 size
    IncreaseSize(1);

    // 3) 更新 child 页的 parent_page_id
    Page *child_page = buffer_pool_manager->FetchPage(value);
    reinterpret_cast<BPlusTreePage *>(child_page)->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, /*is_dirty=*/true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    // 1) 获取当前页最后一个 key-value 对
    int last_idx = GetSize() - 1;
    page_id_t last_value = ValueAt(last_idx);
    GenericKey *last_key = KeyAt(last_idx);

    // 2) 从第一个槽开始，把后面的元素都往后挪一个位置
    for (int i = recipient->GetSize(); i > 0; i--) {
        recipient->SetKeyAt(i, recipient->KeyAt(i - 1));
        recipient->SetValueAt(i, recipient->ValueAt(i - 1));
    }

    recipient->IncreaseSize(1);

    // 3) 把最后一个 key-value 对搬给 recipient
    recipient->SetKeyAt(1, last_key);
    recipient->SetValueAt(1, last_value);

    // 4）把 middle_key 放到 recipient 的第最后一个槽
    recipient->SetKeyAt(GetSize(), middle_key);

    // 5) 更新 recipient 的 size
    recipient->IncreaseSize(1);

    // 6) 更新当前页的 size
    IncreaseSize(-1);

    // 7) 把当前页的最后一个 key-value 对删除
    Remove(last_idx);

    // 8) 更新 recipient 中新添加的 pair 的 child 的 parent_page_id
    Page *child_page_pair = buffer_pool_manager->FetchPage(last_value);
    reinterpret_cast<BPlusTreePage *>(child_page_pair)->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(last_value, /*is_dirty=*/true);

    // 9) 更新 recipient 中 middle key child 的 parent_page_id
    page_id_t child = recipient->ValueAt(recipient->GetSize() - 1);
    Page *child_page_middle = buffer_pool_manager->FetchPage(child);
    reinterpret_cast<BPlusTreePage *>(child_page_middle)->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child, /*is_dirty=*/true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    // 1) 先把后面的元素都往后挪一个位置
    for (int i = GetSize(); i > 0; i--) {
        SetKeyAt(i, KeyAt(i - 1));
        SetValueAt(i, ValueAt(i - 1));
    }

    // 2) 把第一个槽放上新元素
    SetValueAt(0, value);

    // 3) Update size
    IncreaseSize(1);

    // 4) Update parent page id for the child
    Page *child_page = buffer_pool_manager->FetchPage(value);
    reinterpret_cast<BPlusTreePage *>(child_page)->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, /*is_dirty=*/true);
}