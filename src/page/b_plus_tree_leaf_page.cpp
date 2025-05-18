#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetKeySize(key_size);
    SetMaxSize(max_size);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetNextPageId(INVALID_PAGE_ID);               // ← 用 setter
    // 清空所有 slots
    memset(pairs_off, 0, PAGE_SIZE - LEAF_PAGE_HEADER_SIZE);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
    int left = 0;
    int right = GetSize();  // 注意：right 是 “一 past the end”

    while (left < right) {
        int mid = left + (right - left) / 2;
        // 如果 mid 位置上的 key < 目标 key，就往右边去
        if (KM.CompareKeys(KeyAt(mid), key) < 0) {
            left = mid + 1;
        } else {
            // 否则 mid 可能就是答案，或者答案在左半区
            right = mid;
        }
    }
    return left;  // left == right，正是第一个 >= key 的位置
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
    cout << "Enter LeafPage::Insert" << endl;
    // 检查是否已满
    cout << "Size " << GetSize() << " MaxSize " << GetMaxSize() << endl;
    if (GetSize() >= GetMaxSize()) {
        return -1; // 页已满，无法插入
    }

    // 找到插入位置
    int index = KeyIndex(key, KM);
    cout << "index: " << index << endl;

    // 检查是否已存在相同的key
    if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
        return -2; // 已存在相同的key，无法插入
    }

    // 移动后续元素以腾出插入位置
    memmove(pairs_off + (index + 1) * pair_size, pairs_off + index * pair_size,
            (GetSize() - index) * pair_size);

    // 插入新元素
    SetKeyAt(index, key);
    cout << "SetKeyAt: " << KeyAt(index) << endl;
    SetValueAt(index, value);

    // 更新页大小
    IncreaseSize(1);
    cout << "Current size (after insertion): " << GetSize() << endl;

    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
    int half_size = GetSize() / 2;
    // 将后半部分的key-value对复制到recipient
    recipient->CopyNFrom(pairs_off + half_size * pair_size, GetSize() - half_size);
    // 更新当前页的大小
    SetSize(half_size);
    // 更新当前页的next_page_id
    SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
    int old_size = GetSize();
    // 检查是否已满
    if (old_size + size > GetMaxSize()) {
        return; // 页已满，无法插入
    }
    // 计算新的大小
    SetSize(old_size + size);
    // 复制数据
    memcpy(pairs_off + old_size * pair_size, src, size * pair_size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
    cout << "Enter LeafPage::Lookup" << endl;
    // 找到第一个 大于/等于 键的索引
    int index = KeyIndex(key, KM);
    cout << "index: " << index << endl;
    // **边界检查**：如果 index 已经等于当前大小，就说明要插入到尾部，不存在重复
    if (index >= GetSize()) {
        cout << "Key not found index: " << index << " >= Size:" << GetSize() << endl;
        return false;
    }

    // 看看是否存在相同的键
    if (KM.CompareKeys(KeyAt(index), key) == 0) {
        cout << "Found key: " << KeyAt(index) << endl;
        value = ValueAt(index);
        return true; // 找到键
    }
    cout << "Key not found" << endl;
    return false; // 没有找到键
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
    // 找到第一个 大于/等于 键的索引
    int index = KeyIndex(key, KM);

    // 看看是否存在相同的键
    if (KM.CompareKeys(KeyAt(index), key) != 0) {
        return -1; // 没有找到键
    }

    // 删除键值对
    memmove(pairs_off + index * pair_size, pairs_off + (index + 1) * pair_size,
            (GetSize() - index - 1) * pair_size);

    // 更新页大小
    SetSize(GetSize() - 1);

    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
    // 将当前页的所有数据复制到recipient
    recipient->CopyNFrom(pairs_off, GetSize());
    // 更新recipient的next_page_id
    recipient->SetNextPageId(GetNextPageId());
    // 更新当前页的大小
    SetSize(0);
    // 更新当前页的next_page_id
    SetNextPageId(INVALID_PAGE_ID);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
    // 将当前页的第一个元素复制到recipient
    recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
    // 更新当前页的大小
    SetSize(GetSize() - 1);
    // 将当前页的元素向前移动
    memmove(pairs_off, pairs_off + pair_size, GetSize() * pair_size);
    // 更新 recipient 页的next_page_id
    recipient->SetNextPageId(GetPageId());
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
    // 检查是否已满
    if (GetSize() >= GetMaxSize()) {
        return; // 页已满，无法插入
    }
    // 插入新元素
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    // 更新页大小
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
    // 将当前页的最后一个元素复制到recipient
    recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
    // 更新当前页的大小
    SetSize(GetSize() - 1);
    // 更新当前页的next_page_id
    SetNextPageId(recipient->GetPageId());
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
    // 检查是否已满
    if (GetSize() >= GetMaxSize()) {
        return; // 页已满，无法插入
    }
    // 将当前页的元素向后移动
    memmove(pairs_off + pair_size, pairs_off, GetSize() * pair_size);
    // 插入新元素
    SetKeyAt(0, key);
    SetValueAt(0, value);
    // 更新页大小
    IncreaseSize(1);
}