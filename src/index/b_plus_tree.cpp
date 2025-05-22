#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/header_page.h"  // 包含 HeaderPage 的定义

/**
 * TODO: Student Implement
 */



BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
        : root_page_id_(INVALID_PAGE_ID),
          index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          processor_(KM),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size){
    if (leaf_max_size > 0) {
        leaf_max_size_ = leaf_max_size;
    } else {
        leaf_max_size_ = static_cast<int>(
                (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) /
                (processor_.GetKeySize() + sizeof(RowId)));
    }
    // 如果调用时 internal_max_size <= 0，也自动计算默认值
    if (internal_max_size > 0) {
        internal_max_size_ = internal_max_size;
    } else {
        internal_max_size_ = static_cast<int>(
                (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) /
                (processor_.GetKeySize() + sizeof(page_id_t)));
    }

// —— 初始化或加载 header page ——
    Page *hdr = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    if (hdr == nullptr) {
        // header page 不存在，第一次创建
        page_id_t pid;
        hdr = buffer_pool_manager_->NewPage(pid);
        CHECK(hdr != nullptr && pid == INDEX_ROOTS_PAGE_ID);
        auto *header = reinterpret_cast<HeaderPage *>(hdr->GetData());
        header->Init();
        buffer_pool_manager_->UnpinPage(pid, /*is_dirty=*/true);
        root_page_id_ = INVALID_PAGE_ID;
    } else {
        // header page 已存在，读取之前写入的 root_page_id
        auto *header = reinterpret_cast<HeaderPage *>(hdr->GetData());
        page_id_t loaded_root = INVALID_PAGE_ID;
        if (header->GetRootId(std::to_string(index_id_), &loaded_root)) {
            root_page_id_ = loaded_root;
        } else {
            root_page_id_ = INVALID_PAGE_ID;
        }
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, /*is_dirty=*/false);
    }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
    if (current_page_id == INVALID_PAGE_ID) {
        return;
    }
    // 1. Fetch 并 pin 当前页面
    auto *page = buffer_pool_manager_->FetchPage(current_page_id);
    if (page == nullptr) {
        return;
    }
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (tree_page->IsLeafPage()) {
        // --- 叶子页：删除堆上分配的 GenericKey* ---
        auto *leaf = reinterpret_cast<LeafPage *>(tree_page);
        int sz = leaf->GetSize();
        for (int i = 0; i < sz; ++i) {
            GenericKey *key = leaf->KeyAt(i);
            delete key;  // 释放每个 key
        }
    } else {
        // --- 内部页：递归删除所有 n+1 个子页面 ---
        auto *internal = reinterpret_cast<InternalPage *>(tree_page);
        int sz = internal->GetSize();
        for (int i = 0; i <= sz; ++i) {
            page_id_t child_id = internal->ValueAt(i);
            Destroy(child_id);
        }
    }
    // 2. 删除当前页：先 unpin，再 delete
    buffer_pool_manager_->UnpinPage(current_page_id, /*is_dirty=*/false);
    buffer_pool_manager_->DeletePage(current_page_id);
}


/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
    // cout << "root_page_id_: " << root_page_id_ << endl;
    return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
    // cout << "Enter GetValue" << endl;
    // 1. 如果树为空，直接返回 false
    if (IsEmpty()) {
        // cout << "Tree is empty" << endl;
        return false;
    }

    // 2. 定位到包含目标 key 的叶子页并 pin
    Page *page = FindLeafPage(key);
    // cout << "FindLeafPage: " << page->GetPageId() << endl;
    if (page == nullptr) {
        // 没有找到对应的页（极少出现，防御性编程）
        return false;
    }

    // 3. 将原始 Page* 数据区转换为叶子页类型
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());

    // 4. 在叶子页中查找 key
    RowId value;
    bool found = leaf->Lookup(key, value, processor_);

    // 5. 如果找到，就把 RowId 加入结果列表
    if (found) {
        result.push_back(value);
    }

    // 6. unpin 叶子页（查找时 pin，使用后必须 unpin；此处不做修改所以 is_dirty=false）
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), /*is_dirty=*/false);

    // 7. 返回是否命中
    return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
//bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
//    // 1. 如果树为空，直接插入新树
//    if (IsEmpty()) {
//        // cout << "Start new tree" << endl;
//        StartNewTree(key, value);
//        return true;
//    }
//
//    // 2. 定位到包含目标 key 的叶子页并 pin
//    Page *page = FindLeafPage(key);
//    // cout << "FindLeafPage: " << page->GetPageId() << endl;
//    if (page == nullptr) {
//        return false;
//    }
//
//    // 3. 将原始 Page* 数据区转换为叶子页类型
//    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
//    // cout << "Convert to LeafPage: " << leaf->GetPageId() << endl;
//
//
//    // 4. 在叶子页中插入 key 和 value
//    // cout << "Start InsertIntoLeaf" << endl;
//    bool inserted = InsertIntoLeaf(key, value, transaction);
//    // cout << "InsertedSuccess?: " << inserted << endl;
//    // cout << "InsertIntoLeaf: " << leaf->GetPageId() << endl;
//
//    // 5. unpin 叶子页（插入时 pin，使用后必须 unpin；此处不做修改所以 is_dirty=false）
//    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), /*is_dirty=*/true);
//
//    // 6. 返回是否命中
//    return inserted;
//}

bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *txn) {
    if (IsEmpty()) {
        // cout << "Start new tree" << endl;
        StartNewTree(key, value);
        return true;
    }
    // 1 次 pin
    Page *page = FindLeafPage(key);
    if (page == nullptr) return false;
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());

    // 统一让 InsertIntoLeaf 去做插入和可能的 split/new_leaf unpin
    bool ok = InsertIntoLeaf(leaf, key, value, txn);

    // **这里** 对最初 pin 的 leaf 做配对 unpin
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), /*is_dirty=*/ok);
    return ok;
}


/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
    // 1) 正确分配一页并拿到它的 page_id
    page_id_t new_root_id;
    Page *page = buffer_pool_manager_->NewPage(new_root_id);
    if (page == nullptr) {
        throw std::runtime_error("Out of memory");
    }

    // 2) 拿到数据区，初始化叶子节点
    auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    leaf_page->Init(new_root_id,
                    INVALID_PAGE_ID,
                    processor_.GetKeySize(),
                    leaf_max_size_);

    // 3) 插入第一条记录
    leaf_page->Insert(key, value, processor_);

    // 4) **关键**：先更新成员变量
    root_page_id_ = new_root_id;
    // cout << "Update root page id: " << root_page_id_ << endl;


    buffer_pool_manager_->UnpinPage(new_root_id, /*is_dirty=*/true);

    // 5) 再写到 header page（insert_record = true）
    UpdateRootPageId(/*insert_record=*/true);
}


/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
//bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
//    // cout << "Enter InsertIntoLeaf" << endl;
//    // 1. 找到目标叶子页（默认从根开始），并 pin
//    Page *page = FindLeafPage(key, INVALID_PAGE_ID, false);
//    // cout << "FindLeafPage: " << page->GetPageId() << endl;
//    if (page == nullptr) {
//        // 没有可插入的叶子页（极端情况）
//        return false;
//    }
//    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
//
//    // 2. 检查重复：若已存在相同 key，则释放叶子页并返回 false
//    RowId tmp;
//    if (leaf->Lookup(key, tmp, processor_)) {
//        // cout << "Duplicate key!!!" << endl;
//        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), /*is_dirty=*/false);
//        return false;
//    }
//    // cout << "Lookup: " << leaf->GetPageId() << endl;
//
//    // 3. 执行插入
//    int status = leaf->Insert(key, value, processor_);
//    // 序列化一下
//    // processor_.SerializeFromKey(//)
//    // // cout << "Insert: " << leaf->GetPageId() << endl;
//
//    // 4. 如果超出容量，则 split 并向父节点插入分裂出来的新页的第一个 key
//    // if (leaf->GetSize() > leaf_max_size_) {
//    if (status == -1) {
//        // cout << "LeafPage overflow: " << leaf->GetPageId() << endl;
//        // split 会从 buffer pool 中 NewPage 并 pin 新页
//        LeafPage *new_leaf = Split(leaf, transaction);
//        // 提升 new_leaf 中最小的 key 到 parent
//        GenericKey *promote_key = new_leaf->KeyAt(0);
//        // cout << "Promote key: " << promote_key << endl;
//        InsertIntoParent(leaf, promote_key, new_leaf, transaction);
//
//
//        // split 后的新叶子页要 unpin 并标记为脏
//        buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), /*is_dirty=*/true);
//        // InsertIntoLeaf(key, value, transaction);
//
//        // 分裂后决定到底插入到哪一页
//        if (processor_.CompareKeys(key, promote_key) >= 0) {
//            leaf = new_leaf;
//        }
//
//        leaf->Insert(key, value, processor_);
//        buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), /*is_dirty=*/true);
//    }
//
//    // 5. 释放原叶子页并标记为脏
//    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), /*is_dirty=*/true);
//    //// cout << "Unpin old leaf page: " << leaf->GetPageId() << endl;
//    return true;
//}

bool BPlusTree::InsertIntoLeaf(LeafPage *leaf, GenericKey *key,
                               const RowId &value, Txn *txn) {
    // —— 已经 pin 了 leaf，不要再 FindLeafPage ——

    // 1. 检查重复
    RowId tmp;
    if (leaf->Lookup(key, tmp, processor_)) {
        // duplicate：直接 unpin leaf 并返回 false
        // cout << "Duplicate key!!!" << endl;
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), /*is_dirty=*/false);
        return false;
    }

    // 2. 插入
    int status = leaf->Insert(key, value, processor_);

    // 3. 分裂
    if (status == -1) {
        LeafPage *new_leaf = Split(leaf, txn);  // new_leaf 被 pin
        GenericKey *promote = new_leaf->KeyAt(0);
        InsertIntoParent(leaf, promote, new_leaf, txn);
        // 决定最终往哪页插
        if (processor_.CompareKeys(key, promote) >= 0) {
            leaf = new_leaf;
        }
        leaf->Insert(key, value, processor_);
        // **配对 unpin new_leaf**
        buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), /*is_dirty=*/true);
    }

    // **不在这里 unpin 原 leaf**，留给 Insert() 统一处理
    return true;
}



/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
    // 1. 向 BufferPoolManager 请求一页新页面（已 pin）
    page_id_t new_page_id;
    Page *page = buffer_pool_manager_->NewPage(new_page_id);
    if (page == nullptr) {
        throw std::bad_alloc();  // 内存不足
    }

    // 2. 初始化新内部页：继承 parent_id、key_size，使用 internal_max_size_
    auto *new_internal = reinterpret_cast<InternalPage *>(page->GetData());
    new_internal->Init(new_page_id,
                       node->GetParentPageId(),
                       node->GetKeySize(),
                       internal_max_size_);

    // 3. 将 node 的后半部分条目搬到 new_internal（含 child parent_id 更新）
    node->MoveHalfTo(new_internal, buffer_pool_manager_);

    // 4. 返回新页（保持 pin，由调用者在合适时机 unpin 并标记脏页）
    return new_internal;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
    // 1. 申请新页（已 pin）
    // cout << "Split LeafPage: " << node->GetPageId() << endl;
    page_id_t new_page_id;
    Page *page = buffer_pool_manager_->NewPage(new_page_id);
    if (page == nullptr) {
        throw std::bad_alloc();
    }
    auto *new_leaf = reinterpret_cast<LeafPage *>(page->GetData());

    // 2. 初始化新叶子页
    new_leaf->Init(new_page_id,
                   node->GetParentPageId(),
                   node->GetKeySize(),
                   leaf_max_size_);
    // cout << "Init new LeafPage: " << new_leaf->GetPageId() << endl;

    // 3. 先接上原链表：new_leaf.next = node.next
    page_id_t old_next = node->GetNextPageId();
    new_leaf->SetNextPageId(old_next);
    // cout << "SetNextPageId: " << new_leaf->GetNextPageId() << endl;

    // 4. 搬半边元素：内部会做 node.next = new_leaf.page_id
    node->MoveHalfTo(new_leaf);
    // cout << "MoveHalfTo: " << node->GetPageId() << endl;

    // 5. 返回新页（保持 pin，由调用者后面 unpin & 标记脏页）
    return new_leaf;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
    // 原节点页号 & 新节点页号
    page_id_t old_page_id = old_node->GetPageId();
    page_id_t new_page_id = new_node->GetPageId();
    // cout << "InsertIntoParent: old_node " << old_page_id << " new_node " << new_page_id << endl;

    // 1) 如果 old_node 是根节点，则要新建一个根内部页
    if (old_node->IsRootPage()) {
        // a. 申请新页（已 pin）
        // cout << "Create new root page" << endl;
        page_id_t new_root_id;
        Page *p = buffer_pool_manager_->NewPage(new_root_id);
        if (p == nullptr) throw std::bad_alloc();
        auto *root = reinterpret_cast<InternalPage *>(p->GetData());

        // b. 初始化为内部页，parent = INVALID
        root->Init(new_root_id,
                   INVALID_PAGE_ID,
                   old_node->GetKeySize(),
                   internal_max_size_);

        // c. 将 old_node 和 new_node 连接到新根，并插入 key
        root->PopulateNewRoot(old_page_id, key, new_page_id);
        // cout << "PopulateNewRoot: " << root->GetPageId() << endl;

        // d. 更新两棵子树的 parent_page_id
        old_node->SetParentPageId(new_root_id);
        new_node->SetParentPageId(new_root_id);
        // cout << "SetParentPageId: old_node " << old_node->GetPageId() << " new_node " << new_node->GetPageId() << endl;

        // e. 释放子树页 pin，标记为脏
        buffer_pool_manager_->UnpinPage(old_page_id, /*is_dirty=*/true);
        buffer_pool_manager_->UnpinPage(new_page_id, /*is_dirty=*/true);

        // f. 更新树的根指针 & 写 header
        root_page_id_ = new_root_id;
        // cout << "Update root page id: " << root_page_id_ << endl;

        UpdateRootPageId(/*insert_record=*/true);
        // cout << "Update header page" << endl;

        // g. 释放新根页 pin
        buffer_pool_manager_->UnpinPage(new_root_id, /*is_dirty=*/true);

        return;
    }

    // 2) 非根节点：找到父节点并 pin
    page_id_t parent_page_id = old_node->GetParentPageId();
    Page *p = buffer_pool_manager_->FetchPage(parent_page_id);
    if (p == nullptr) throw std::runtime_error("Failed to fetch parent page");
    auto *parent = reinterpret_cast<InternalPage *>(p->GetData());

    // 3) 在 parent 中，old_page_id 之后插入 <key, new_page_id>
    int new_size = parent->InsertNodeAfter(old_page_id, key, new_page_id);
    // 为新节点更新 parent_page_id
    new_node->SetParentPageId(parent_page_id);

    // 4) 如果 parent 溢出，split 并递归上溢
    if (new_size > internal_max_size_) {
        // a. split parent（返回已 pin 的新内部页）
        InternalPage *new_internal = Split(parent, transaction);
        // 推上去的分隔 key 就是 new_internal->KeyAt(0)
        GenericKey *up_key = new_internal->KeyAt(0);

        // b. 先 unpin 原父页
        buffer_pool_manager_->UnpinPage(parent_page_id, /*is_dirty=*/true);

        // c. 递归：把 up_key 和 new_internal 插入上一层
        InsertIntoParent(parent, up_key, new_internal, transaction);

        // d. 释放新内部页 pin
        buffer_pool_manager_->UnpinPage(new_internal->GetPageId(), /*is_dirty=*/true);
    } else {
        // 5) 没有溢出，直接 unpin 父页并标记脏
        buffer_pool_manager_->UnpinPage(parent_page_id, /*is_dirty=*/true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
    // 1. 空树直接返回
    if (IsEmpty()) {
        return;
    }

    // 2. 定位到叶子页并 pin
    Page *page = FindLeafPage(key);
    if (page == nullptr) {
        return;
    }
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    page_id_t leaf_page_id = leaf->GetPageId();

    // 3. 在叶子页中删除记录
    int deleted_cnt = leaf->RemoveAndDeleteRecord(key, processor_);
    if (deleted_cnt <= 0) {
        // key 不存在，无需修改
        buffer_pool_manager_->UnpinPage(leaf_page_id, /*is_dirty=*/false);
        return;
    }

    // 4. 如果叶子页是根节点
    if (leaf->IsRootPage()) {
        if (leaf->GetSize() == 0) {
            // 整棵树删除完毕
            buffer_pool_manager_->UnpinPage(leaf_page_id, /*is_dirty=*/false);
            buffer_pool_manager_->DeletePage(leaf_page_id);
            root_page_id_ = INVALID_PAGE_ID;
            // 从 header 中移除记录
            UpdateRootPageId(/*insert_record=*/0);
        } else {
            // 根叶子页还剩元素，只需标脏
            buffer_pool_manager_->UnpinPage(leaf_page_id, /*is_dirty=*/true);
        }
        return;
    }

    // 5. 非根叶子页：检测下溢
    int min_size = leaf->GetMinSize();
    bool deleted = false;
    if (leaf->GetSize() < min_size) {
        // 合并或重分配
        deleted = CoalesceOrRedistribute<LeafPage>(leaf, transaction);
    }

    // 6. unpin & 标脏
    // 如果通过合并真正删除了该页，CoalesceOrRedistribute 内部会做 DeletePage 操作，
    // 此处只需 unpin 可能残留的页即可。
    buffer_pool_manager_->UnpinPage(leaf_page_id, /*is_dirty=*/true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
    // 1. 如果是根节点，调用 AdjustRoot 并返回其结果
    if (node->IsRootPage()) {
        return AdjustRoot(node);
    }

    // 2. Fetch 父节点
    page_id_t parent_id = node->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

    // 3. 找到 node 在 parent 中的下标
    int index = parent->ValueIndex(node->GetPageId());

    // 4. Fetch 兄弟节点（左或右），并 pin
    N *sibling = nullptr;
    Page *sibling_page = nullptr;
    if (index == 0) {
        // 没有左兄弟，拿右兄弟
        page_id_t sid = parent->ValueAt(1);
        sibling_page = buffer_pool_manager_->FetchPage(sid);
        sibling = reinterpret_cast<N *>(sibling_page->GetData());
    } else {
        // 否则拿左兄弟
        page_id_t sid = parent->ValueAt(index - 1);
        sibling_page = buffer_pool_manager_->FetchPage(sid);
        sibling = reinterpret_cast<N *>(sibling_page->GetData());
    }

    // 5. 决定合并还是重分配
    int combined_size = sibling->GetSize() + node->GetSize();
    if (combined_size <= node->GetMaxSize()) {
        // 合并：会在 Coalesce 内部删除 node 或 sibling、更新 parent
        bool parent_underflow = Coalesce(sibling, node, parent, index, transaction);
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        if (parent_underflow) {
            return CoalesceOrRedistribute<InternalPage>(parent, transaction);
        }
        return false;
    } else {
        // 重分配：会在 Redistribute 内部更新 parent 的分隔 key
        Redistribute(sibling, node, index);
        buffer_pool_manager_->UnpinPage(sibling->GetPageId(), /*is_dirty=*/true);
        buffer_pool_manager_->UnpinPage(parent_id, /*is_dirty=*/true);
        return false;
    }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
    // leaf 合并：将一个页的数据搬到另一个页，然后删掉空页，更新 parent
    if (index == 0) {
        // node 是第0个 child，neighbor_node 是右兄弟
        // 把右兄弟的数据搬到 node（左页）
        neighbor_node->MoveAllTo(node);
        // 删除右兄弟所在页
        buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
        // parent 中移除第1个 key+pointer
        parent->Remove(1);
    } else {
        // neighbor_node 是左兄弟，node 是右页
        // 把 node 的数据搬到左兄弟
        node->MoveAllTo(neighbor_node);
        buffer_pool_manager_->DeletePage(node->GetPageId());
        // parent 中移除对应于 node 的 entry
        parent->Remove(index);
    }
    // 返回父页是否下溢，需要上层继续合并/重分配
    return parent->GetSize() < parent->GetMinSize();
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
    // internal 合并：搬移 key+child pointers，并把分隔 key 也合并过去
    if (index == 0) {
        // node 是第0个 child，neighbor_node 是右兄弟
        GenericKey *sep_key = parent->KeyAt(1);
        // 把右兄弟的数据 + sep_key 搬到 node（左页）
        neighbor_node->MoveAllTo(node, sep_key, buffer_pool_manager_);
        buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
        parent->Remove(1);
    } else {
        // neighbor_node 是左兄弟，node 是右页
        GenericKey *sep_key = parent->KeyAt(index);
        // 把 node 的数据 + sep_key 搬到左兄弟
        node->MoveAllTo(neighbor_node, sep_key, buffer_pool_manager_);
        buffer_pool_manager_->DeletePage(node->GetPageId());
        parent->Remove(index);
    }
    // 返回父页是否下溢，需要上层继续合并/重分配
    return parent->GetSize() < parent->GetMinSize();
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
    // 1) 取父节点
    page_id_t parent_id = node->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

    if (index == 0) {
        // node 是最左叶子，只能从右兄弟借第一个元素
        neighbor_node->MoveFirstToEndOf(node);
        // 更新 parent 分隔键：parent->KeyAt(1) 对应 separator between child0 & child1
        parent->SetKeyAt(1, neighbor_node->KeyAt(0));
    } else {
        // node 不是最左叶子，从左兄弟借最后一个元素
        neighbor_node->MoveLastToFrontOf(node);
        // 更新 parent 分隔键：parent->KeyAt(index) 对应 separator between child(index-1)&child(index)
        parent->SetKeyAt(index, node->KeyAt(0));
    }

    // 2) unpin 父页并标记脏
    buffer_pool_manager_->UnpinPage(parent_id, /*is_dirty=*/true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
    // 1) 取父节点
    page_id_t parent_id = node->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

    if (index == 0) {
        // node 是最左内部节点，只能从右兄弟借第一个 entry
        GenericKey *sep_key = parent->KeyAt(1);
        neighbor_node->MoveFirstToEndOf(node, sep_key, buffer_pool_manager_);
        // 更新 parent 分隔键
        parent->SetKeyAt(1, neighbor_node->KeyAt(1));
    } else {
        // node 不是最左内部节点，从左兄弟借最后一个 entry
        GenericKey *sep_key = parent->KeyAt(index);
        neighbor_node->MoveLastToFrontOf(node, sep_key, buffer_pool_manager_);
        // 更新 parent 分隔键
        parent->SetKeyAt(index, node->KeyAt(1));
    }

    // 2) unpin 父页并标记脏
    buffer_pool_manager_->UnpinPage(parent_id, /*is_dirty=*/true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
    page_id_t old_root_id = old_root_node->GetPageId();

    // --- 情况 1: 根是叶子页 ---
    if (old_root_node->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(old_root_node);
        // 如果删除后叶子页空了，就删掉根
        if (leaf->GetSize() == 0) {
            // 删除根页
            buffer_pool_manager_->DeletePage(old_root_id);
            // 更新树为空
            root_page_id_ = INVALID_PAGE_ID;
            // 同步到 header page
            UpdateRootPageId(/*insert_record=*/0);
            return true;  // 根已被删除
        }
        // 叶子根还剩内容，无需调整
        return false;
    }

    // --- 情况 2: 根是内部页 ---
    auto *root_internal = reinterpret_cast<InternalPage *>(old_root_node);
    // 如果内部根只剩一个子指针，则降高
    if (root_internal->GetSize() == 1) {
        // 拿到唯一的 child page id
        page_id_t child_id = root_internal->RemoveAndReturnOnlyChild();
        // 删除旧根
        buffer_pool_manager_->DeletePage(old_root_id);
        // 设置新的 root_page_id_
        root_page_id_ = child_id;
        // cout << "Update root page id: " << root_page_id_ << endl;
        // 更新新的根在 header page
        UpdateRootPageId(/*insert_record=*/0);
        // 把新根的 parent_id 设为 INVALID
        Page *child_page = buffer_pool_manager_->FetchPage(child_id);
        auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_node->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(child_id, /*is_dirty=*/true);
        return true;  // 根已被替换
    }

    // 其余情况无需调整
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
    // 1. 找到最左叶子页并 pin
    Page *page = FindLeafPage(nullptr, INVALID_PAGE_ID, /*leftMost=*/true);
    if (page == nullptr) {
        // 空树，返回 end iterator
        return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
    }
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    page_id_t pid = leaf->GetPageId();

    // 2. unpin 刚才 pin 的页，让 iterator 自行 fetch
    buffer_pool_manager_->UnpinPage(pid, /*is_dirty=*/false);

    // 3. 由页 id、buffer pool 和起始索引 0 构造 iterator
    return IndexIterator(pid, buffer_pool_manager_, /*index=*/0);
}


/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
    // 1. 定位到包含 key 的叶子页并 pin
    Page *page = FindLeafPage(key, INVALID_PAGE_ID, /*leftMost=*/false);
    if (page == nullptr) {
        // 如果没找到，返回 end iterator
        return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
    }
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    page_id_t pid = leaf->GetPageId();

    // 2. 在叶子页内找到从哪里开始迭代
    int idx = leaf->KeyIndex(key, processor_);

    // 3. unpin 该页，后续 iterator 会自行 re-pin
    buffer_pool_manager_->UnpinPage(pid, /*is_dirty=*/false);

    // 4. 构造并返回 iterator
    return IndexIterator(pid, buffer_pool_manager_, idx);
}


/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
    // 使用无效的 page_id 标识迭代结束
    return IndexIterator();
}


/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
    // 1. 如果整棵树为空，直接返回 nullptr
    if (root_page_id_ == INVALID_PAGE_ID) {
        return nullptr;
    }

    // 2. 确定起点 page_id：若用户未指定，则从 root_page_id_ 开始
    page_id_t cur_page_id = (page_id == INVALID_PAGE_ID ? root_page_id_ : page_id);
    Page *page = buffer_pool_manager_->FetchPage(cur_page_id);
    if (page == nullptr) {
        return nullptr;
    }

    // 3. 反复向下，直到到达叶子页
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    while (!node->IsLeafPage()) {
        auto *internal = reinterpret_cast<InternalPage *>(node);
        page_id_t next_page_id;
        if (leftMost) {
            // 若要求最左叶子，始终沿第 0 个 child 指针下钻
            next_page_id = internal->ValueAt(0);
        } else {
            // 否则根据 key 的大小，调用 InternalPage::Lookup 找到应该下钻的 child 下标
            next_page_id = internal->Lookup(key, processor_);
        }
        // 将当前内部页 unpin 后，再读取下一个 child
        buffer_pool_manager_->UnpinPage(page->GetPageId(), /*is_dirty=*/false);
        page = buffer_pool_manager_->FetchPage(next_page_id);
        if (page == nullptr) {
            return nullptr;
        }
        node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }

    // 4. 此时 page 指向叶子页，并且仍处于 pin 状态。unpin 在调用者处完成
    return page;
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page is defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */



void BPlusTree::UpdateRootPageId(int insert_record) {
    // 1. 从 buffer pool 中拿到 header page，并 pin
    Page *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    if (page == nullptr) {
        LOG(FATAL) << "Cannot fetch INDEX_ROOTS_PAGE_ID page";
        return;
    }

    // 2. 互斥写锁，防止并发修改
    page->WLatch();

    // 3. 拿到 HeaderPage 对象
    auto *header = reinterpret_cast<HeaderPage *>(page->GetData());
    header->Init();

    const std::string index_name = std::to_string(index_id_);

    // 4. 根据 insert_record 调用对应接口
    bool ok = false;
    if (insert_record) {
        ok = header->InsertRecord(index_name, root_page_id_);
    } else {
        ok = header->UpdateRecord(index_name, root_page_id_);
    }
    if (!ok) {
        LOG(ERROR) << (insert_record ? "InsertRecord" : "UpdateRecord")
                   << " failed for index " << index_name;
    }

    // 5. 解写锁
    page->WUnlatch();

    // 6. unpin 并标记为脏页，让 buffer pool 持久化到 disk
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, /*is_dirty=*/true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
    std::string leaf_prefix("LEAF_");
    std::string internal_prefix("INT_");
    if (page->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(page);
        // Print node name
        out << leaf_prefix << leaf->GetPageId();
        // Print node properties
        out << "[shape=plain color=green ";
        // Print data of the node
        out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
        // Print data
        out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
            << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
        out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
            << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
            << "</TD></TR>\n";
        out << "<TR>";
        for (int i = 0; i < leaf->GetSize(); i++) {
            Row ans;
            processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
            out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
        }
        out << "</TR>";
        // Print table end
        out << "</TABLE>>];\n";
        // Print Leaf node link if there is a next page
        if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
            out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
            out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
        }

        // Print parent links if there is a parent
        if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
            out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
                << leaf->GetPageId() << ";\n";
        }
    } else {
        auto *inner = reinterpret_cast<InternalPage *>(page);
        // Print node name
        out << internal_prefix << inner->GetPageId();
        // Print node properties
        out << "[shape=plain color=pink ";  // why not?
        // Print data of the node
        out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
        // Print data
        out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
            << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
        out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
            << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
            << "</TD></TR>\n";
        out << "<TR>";
        for (int i = 0; i < inner->GetSize(); i++) {
            out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
            if (i > 0) {
                Row ans;
                processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
                out << ans.GetField(0)->toString();
            } else {
                out << " ";
            }
            out << "</TD>\n";
        }
        out << "</TR>";
        // Print table end
        out << "</TABLE>>];\n";
        // Print Parent link
        if (inner->GetParentPageId() != INVALID_PAGE_ID) {
            out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
                << inner->GetPageId() << ";\n";
        }
        // Print leaves
        for (int i = 0; i < inner->GetSize(); i++) {
            auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
            ToGraph(child_page, bpm, out, schema);
            if (i > 0) {
                auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
                if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
                    out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
                        << child_page->GetPageId() << "};\n";
                }
                bpm->UnpinPage(sibling_page->GetPageId(), false);
            }
        }
    }
    bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
    if (page->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(page);
        // std::// cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
                  // << " next: " << leaf->GetNextPageId() << std::endl;
        for (int i = 0; i < leaf->GetSize(); i++) {
            // std::// cout << leaf->KeyAt(i) << ",";
        }
        // std::// cout << std::endl;
        // std::// cout << std::endl;
    } else {
        auto *internal = reinterpret_cast<InternalPage *>(page);
        // std::// cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
        for (int i = 0; i < internal->GetSize(); i++) {
            // std::// cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
        }
        // std::// cout << std::endl;
        // std::// cout << std::endl;
        for (int i = 0; i < internal->GetSize(); i++) {
            ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
            bpm->UnpinPage(internal->ValueAt(i), false);
        }
    }
}

bool BPlusTree::Check() {
    bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
    if (!all_unpinned) {
        LOG(ERROR) << "problem in page unpin" << endl;
    }
    return all_unpinned;
}