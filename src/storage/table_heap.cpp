#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    // 1. 尝试在已有页面中插
    page_id_t cur_pid = first_page_id_;
    page_id_t prev_pid = INVALID_PAGE_ID;  // 记录上一次的非空页
    while (cur_pid != INVALID_PAGE_ID) {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_pid));
        if (!page) return false;
        page->WLatch();
        bool inserted = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_pid, inserted);
        if (inserted) return true;

        // 记录当前页号，然后跳到下一个
        prev_pid = cur_pid;
        cur_pid = page->GetNextPageId();
    }

    // 2. 所有旧页都满了，prev_pid 正好是最后一页的页号
    page_id_t new_pid;
    Page *raw = buffer_pool_manager_->NewPage(new_pid);
    if (!raw) return false;
    auto new_page = reinterpret_cast<TablePage *>(raw);
    // 用 prev_pid 初始化新页
    new_page->Init(new_pid, prev_pid, log_manager_, txn);

    // 3. 如果 prev_pid 有效，就把它的 next 指向 new_pid
    if (prev_pid != INVALID_PAGE_ID) {
        auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_pid));
        prev_page->WLatch();
        prev_page->SetNextPageId(new_pid);
        prev_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(prev_pid, /*is_dirty=*/true);
    }

    // 4. 向新页插入
    new_page->WLatch();
    bool ok = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    new_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_pid, ok);
    return ok;
}



bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    // 找到rid所在的页
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return false;
    }
    // 给old_row赋值
    Row old_row;
    page->WLatch();
    bool ok = page->GetTuple(&old_row, schema_, txn, lock_manager_);
    page->WUnlatch();
    if (!ok) {
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        return false;
    }

    // 尝试更新
    page->WLatch();
    ok = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), ok);
    if (!ok) {
        // 如果更新失败，说明新行太大了，先删除旧行
        MarkDelete(rid, txn);
        // 然后插入新行
        ok = InsertTuple(row, txn);
        if (!ok) {
            // 插入失败，回滚
            RollbackDelete(rid, txn);
        }
        return ok;
    }
    // 更新成功
    // 将RowId为rid的记录old_row替换成新的记录new_row，并将new_row的RowId通过new_row.rid_返回
    // 这里的old_row是一个临时变量，不能直接返回
    // 需要将old_row的值复制到row中
    row.SetRowId(rid);
    return true;

}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return;
    }
    // Step2: Delete the tuple from the page.
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    // 先找到row对应的页
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    if (page == nullptr) {
        return false;
    }
    // 然后从页中获取行
    page->RLatch();
    bool ok = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return ok;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
    page_id_t pid = first_page_id_;
    // 遍历链表中所有页面，找到第一个有 tuple 的位置
    while (pid != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid));
    if (page == nullptr) {
        break;  // 无法取到页，直接退出
    }
    // 锁住页面，找第一个 tuple
    page->RLatch();
    RowId first_rid;
    bool ok = page->GetFirstTupleRid(&first_rid);
    page->RUnlatch();
    // 释放页面引用，不标记脏
    buffer_pool_manager_->UnpinPage(pid, /*is_dirty=*/false);

    if (ok) {
        // 找到合法的 tuple，返回指向它的迭代器
        return TableIterator(this, first_rid, txn);
    }
    // 否则跳到下一页继续
    pid = page->GetNextPageId();
    }
    // 整个表都没有 tuple，返回 end()
    return End();
}


/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
    // 返回一个空的迭代器
    return TableIterator(nullptr, RowId(), nullptr);
}
