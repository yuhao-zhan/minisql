#include "index/b_plus_tree_index.h"

#include "index/generic_key.h"
#include "utils/tree_file_mgr.h"
#include <algorithm>
BPlusTreeIndex::BPlusTreeIndex(index_id_t        index_id,
                               IndexSchema      *key_schema,
                               size_t            key_size,
                               BufferPoolManager *buffer_pool_manager)
        : Index(index_id, key_schema),
          processor_(key_schema, key_size),
        // 直接在这里计算并传入 leaf_max_size 和 internal_max_size
          container_(
                  index_id,
                  buffer_pool_manager,
                  processor_,
                  /* leaf_max_size = */ static_cast<int>(
                          (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (key_size + sizeof(RowId))
                  ),
                  /* internal_max_size = */ static_cast<int>(
                          (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (key_size + sizeof(page_id_t))
                  )
          ) {
    // 其余初始化保持不变
}

dberr_t BPlusTreeIndex::InsertEntry(const Row &key, RowId row_id, Txn *txn) {
  // ASSERT(row_id.Get() != INVALID_ROWID.Get(), "Invalid row id for index insert.");
  Row key_with_rid = key;
  key_with_rid.SetRowId(row_id);
  GenericKey *index_key = processor_.InitKey();
  cout << "Start insert index key: " << row_id.GetPageId() << ", " << row_id.GetSlotNum() << endl;
  processor_.SerializeFromKey(index_key, key_with_rid, key_schema_);
  cout << "index_key after processor_.SerializeFromKey: " << index_key << endl;
  cout << "Serialized key: " << key_with_rid.GetRowId().GetPageId() << ", " << key_with_rid.GetRowId().GetSlotNum() << endl;

  bool status = container_.Insert(index_key, row_id, txn);

  cout << "End insert index key: " << row_id.GetPageId() << ", " << row_id.GetSlotNum() << endl;
  //  TreeFileManagers mgr("tree_");
  //  static int i = 0;
  //  if (i % 10 == 0) container_.PrintTree(mgr[i]);
  //  i++;

  if (!status) {
      cout << "Failed to insert index key: " << row_id.GetPageId() << ", " << row_id.GetSlotNum() << endl;
      free(index_key);
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t BPlusTreeIndex::RemoveEntry(const Row &key, RowId row_id, Txn *txn) {
  GenericKey *index_key = processor_.InitKey();
  processor_.SerializeFromKey(index_key, key, key_schema_);

  container_.Remove(index_key, txn);
  free(index_key);
  return DB_SUCCESS;
}

dberr_t BPlusTreeIndex::ScanKey(const Row &key, vector<RowId> &result, Txn *txn, string compare_operator) {
  GenericKey *index_key = processor_.InitKey();
  processor_.SerializeFromKey(index_key, key, key_schema_);
  cout << "Start scan index key: " << key.GetRowId().GetPageId() << ", " << key.GetRowId().GetSlotNum() << endl;
  auto end_iter = GetEndIterator();
  cout << "index_key after processor_.SerializeFromKey: " << index_key << endl;
  cout << "Compare operator: " << compare_operator << endl;
  if (compare_operator == "=") {
    container_.GetValue(index_key, result, txn);
  } else if (compare_operator == ">") {
    auto iter = GetBeginIterator(index_key);
    if (container_.GetValue(index_key, result, txn)) ++iter;
    result.clear();
    for (; iter != end_iter; ++iter) {
      result.emplace_back((*iter).second);
    }
  } else if (compare_operator == ">=") {
    for (auto iter = GetBeginIterator(index_key); iter != end_iter; ++iter) {
      result.emplace_back((*iter).second);
    }
  } else if (compare_operator == "<") {
    auto stop_iter = GetBeginIterator(index_key);
    for (auto iter = GetBeginIterator(); iter != stop_iter; ++iter) {
      result.emplace_back((*iter).second);
    }
  } else if (compare_operator == "<=") {
    auto stop_iter = GetBeginIterator(index_key);
    for (auto iter = GetBeginIterator(); iter != stop_iter; ++iter) {
      result.emplace_back((*iter).second);
    }
    container_.GetValue(index_key, result, txn);
  } else if (compare_operator == "<>") {
    for (auto iter = GetBeginIterator(); iter != end_iter; ++iter) {
      result.emplace_back((*iter).second);
    }
    vector<RowId> temp;
    if (container_.GetValue(index_key, temp, txn))
      result.erase(find(result.begin(), result.end(), temp[0]));
  }
  free(index_key);
  if (!result.empty())
    return DB_SUCCESS;
  else
    return DB_KEY_NOT_FOUND;
}

dberr_t BPlusTreeIndex::Destroy() {
  container_.Destroy();
  return DB_SUCCESS;
}

IndexIterator BPlusTreeIndex::GetBeginIterator() {
  return container_.Begin();
}

IndexIterator BPlusTreeIndex::GetBeginIterator(GenericKey *key) {
  return container_.Begin(key);
}

IndexIterator BPlusTreeIndex::GetEndIterator() {
  return container_.End();
}