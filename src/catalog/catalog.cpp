#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
    uint32_t size = 4 + 4 + 4;
    for (auto iter : table_meta_pages_) {
        size += 4 + 4;
    }
    for (auto iter : index_meta_pages_) {
        size += 4 + 4;
    }
    return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager),catalog_meta_(nullptr),
    next_table_id_(0), next_index_id_(0)  {
    if (init) {
        // --- 全新初始化 ---
        // 1) 新建一个空的 CatalogMeta
        catalog_meta_ = CatalogMeta::NewInstance();
        // 2) 在 page 0 上分配并写入
        page_id_t meta_pid = CATALOG_META_PAGE_ID;
        Page *page = buffer_pool_manager_->FetchPage(meta_pid);
        ASSERT(meta_pid == CATALOG_META_PAGE_ID, "Not CATALOG_META_PAGE_ID");  // 确保我们是第 0 页
        char *buf = page->GetData();
        catalog_meta_->SerializeTo(buf);
        // 3) 释放并标记脏页
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, /*is_dirty=*/true);

        // 4) 初始化自增 ID
        next_table_id_.store(catalog_meta_->GetNextTableId());
        next_index_id_.store(catalog_meta_->GetNextIndexId());
    } else {
        // --- 重新打开已有数据库，加载元数据 ---
        Page *page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        ASSERT(page != nullptr, "Failed to fetch catalog meta page.");
        char *buf = page->GetData();
        catalog_meta_ = CatalogMeta::DeserializeFrom(buf);
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, /*is_dirty=*/false);

        // 1) 恢复自增 ID
        next_table_id_.store(catalog_meta_->GetNextTableId());
        next_index_id_.store(catalog_meta_->GetNextIndexId());

        // 2) 加载所有表
        for (auto &pr : *catalog_meta_->GetTableMetaPages()) {
            table_id_t table_id = pr.first;
            page_id_t  page_id  = pr.second;
            if (auto err = LoadTable(table_id, page_id); err != DB_SUCCESS) {
                LOG(ERROR) << "Failed to load table " << table_id;
            }
        }
        // 3) 加载所有索引
        for (auto &pr : *catalog_meta_->GetIndexMetaPages()) {
            index_id_t index_id = pr.first;
            page_id_t  page_id  = pr.second;
            if (auto err = LoadIndex(index_id, page_id); err != DB_SUCCESS) {
                LOG(ERROR) << "Failed to load index " << index_id;
            }
        }
    }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name,
                                    TableSchema *schema,
                                    Txn *txn,
                                    TableInfo *&table_info) {
    // 1) pick a new table ID
    table_id_t table_id = next_table_id_.fetch_add(1);

    // 2) allocate a page in the *catalog* file to hold your TableMetadata
    page_id_t meta_page_id;
    Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);
    if (meta_page == nullptr) return DB_FAILED;
    ASSERT(meta_page_id != CATALOG_META_PAGE_ID,
           "Catalog metadata page collision");

    // 3) deep-copy the schema
    auto schema_copy = Schema::DeepCopySchema(schema);

    // 4) create the *data* heap — this will internally call NewPage + Init()
    auto *heap = TableHeap::Create(buffer_pool_manager_,
                                   schema_copy,
                                   txn,
                                   log_manager_,
                                   lock_manager_);
    // now heap->GetFirstPageId() is a fully Init()’d page
    page_id_t first_data_page = heap->GetFirstPageId();

    // 5) create your TableMetadata with the *data* page ID
    auto *tbl_meta = TableMetadata::Create(table_id,
                                           table_name,
                                           first_data_page,
                                           schema_copy);

    // 6) serialize that metadata out to the catalog page
    tbl_meta->SerializeTo(meta_page->GetData());
    buffer_pool_manager_->UnpinPage(meta_page_id, /*is_dirty=*/true);

    // 7) wire up your in-memory maps
    table_info = TableInfo::Create();
    table_info->Init(tbl_meta, heap);
    tables_[table_id]      = table_info;
    table_names_[table_name] = table_id;
    catalog_meta_->GetTableMetaPages()->emplace(table_id, meta_page_id);

    // 8) persist the updated catalog‐meta
    FlushCatalogMetaPage();

    // ───────────────────────────────────────────────
    // 9) For each column in the schema that is declared UNIQUE, create a single‐column index
    //
    //    We assume your Column class has a method bool IsUnique() or similar.
    //    We also assume the “index_name” convention can be: tableName_columnName_uqidx.
    //
    auto copied_schema = table_info->GetSchema();  // this is the deep copy
    for (uint32_t col_idx = 0; col_idx < copied_schema->GetColumnCount(); col_idx++) {
        const Column *col = copied_schema->GetColumn(col_idx);
        if (col->IsUnique()) {
            // build a vector with exactly this column index
            std::vector<std::string> key_columns{col->GetName()};
            std::string            unique_index_name =
                    table_name + "_" + col->GetName() + "_uqidx";

            IndexInfo *dummy_idx_info = nullptr;
            dberr_t   result = CreateIndex(
                    table_name,                // existing table
                    unique_index_name,         // “table_col_uqidx”
                    key_columns,               // just { columnName }
                    txn,                       // same transaction
                    dummy_idx_info,            // out parameter
                    "BPlusTree");              // or whatever index‐type you default to
            if (result != DB_SUCCESS) {
                // In a real implementation, you might choose to rollback table creation
                LOG(ERROR) << "Failed to create UNIQUE index on column "
                           << col->GetName() << " of table " << table_name;
                return result;
            }
        }
    }

    return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
    // 查找表 ID
    auto it = table_names_.find(table_name);
    if (it == table_names_.end()) return DB_TABLE_NOT_EXIST;
    table_id_t table_id = it->second;
    // 可以直接调用最下面的 GetTable 函数，通过表 ID 获取表信息
    if (GetTable(table_id, table_info) == DB_FAILED) {
        return DB_FAILED;
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
    for (auto &pr : tables_) {
        tables.push_back(pr.second);
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
    // 1) 查找表 ID
    auto it = table_names_.find(table_name);
    if (it == table_names_.end()) return DB_TABLE_NOT_EXIST;
    table_id_t table_id = it->second;
    // 2) 查找表信息
    auto it2 = tables_.find(table_id);
    if (it2 == tables_.end()) return DB_FAILED;
    TableInfo *table_info = it2->second;
    // 3) 生成索引 ID 和页 ID
    index_id_t index_id = next_index_id_.fetch_add(1);
    page_id_t page_id;
    Page *page = buffer_pool_manager_->NewPage(page_id);
    if (page == nullptr) return DB_FAILED;
    ASSERT(page_id != CATALOG_META_PAGE_ID, "Create A Page with PageID = CATALOG_META_PAGE_ID");  // 确保我们不是在第 0 页

    // 4) 把列名转成 key_map
    std::vector<uint32_t> key_map;
    key_map.reserve(index_keys.size());
    auto schema = table_info->GetSchema();
    for (auto &col_name : index_keys) {
        uint32_t col_idx;
        // 假设 Schema::GetColumnIndex 返回 DB_SUCCESS/DB_COLUMN_NAME_NOT_EXIST
        if (schema->GetColumnIndex(col_name, col_idx) != DB_SUCCESS) {
            return DB_COLUMN_NAME_NOT_EXIST;
        }
        key_map.push_back(col_idx);
    }

    // If index_name already exists, return DB_INDEX_NAME_EXIST
    auto it3 = index_names_.find(table_name);
    if (it3 != index_names_.end()) {
        auto it4 = it3->second.find(index_name);
        if (it4 != it3->second.end()) {
            return DB_INDEX_ALREADY_EXIST;
        }
    }

    // 4) 创建索引元数据
    IndexMetadata *idx_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map);
    // 5) 创建索引信息
    index_info = IndexInfo::Create();
    index_info->Init(idx_meta, table_info, buffer_pool_manager_);
    // 6) 将索引信息保存到 maps
    indexes_[index_id] = index_info;
    index_names_[table_name][index_name] = index_id;
    // 7) 将索引元数据序列化到页中
    char *buf = page->GetData();
    idx_meta->SerializeTo(buf);
    buffer_pool_manager_->UnpinPage(page_id, /*is_dirty=*/true);

    // 8) 更新 catalog_meta_ 中的索引元数据页
    catalog_meta_->GetIndexMetaPages()->emplace(index_id, page_id);
    // 9) 更新 catalog_meta_ 页
    FlushCatalogMetaPage();

    TableHeap *table_heap = table_info->GetTableHeap();
    // 取得只包含 index_keys 列的 key_schema
    IndexSchema *key_schema = index_info->GetIndexKeySchema();
    // 原表完整的 schema
    const Schema *orig_schema = table_info->GetSchema();

    for (TableIterator it_table = table_heap->Begin(txn);
         it_table != table_heap->End();
         ++it_table) {
        // operator*() 返回当前行的 Row
        Row table_row = *it_table;
        RowId rid = table_row.GetRowId();

        // 用 Row::GetKeyFromRow 从整行 table_row 中抽出 key_schema 对应的那些列，生成一个新的 Row key_row
        Row key_row;  // 空的 Row，用来存放“索引列”那几列
        table_row.GetKeyFromRow(orig_schema, key_schema, key_row);

        // 把 (key_row, rid) 插入到 B+ 树里
        index_info->GetIndex()->InsertEntry(key_row, rid, txn);
    }

    // 10) 返回索引信息
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
    auto it1 = index_names_.find(table_name);
    if (it1 == index_names_.end()) return DB_FAILED;
    auto it2 = it1->second.find(index_name);
    if (it2 == it1->second.end()) return DB_FAILED;
    index_id_t index_id = it2->second;
    // 3) 查找索引信息
    auto it3 = indexes_.find(index_id);
    if (it3 == indexes_.end()) return DB_FAILED;
    index_info = it3->second;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
    auto it = index_names_.find(table_name);
    if (it == index_names_.end()) return DB_FAILED;
    for (auto &pr : it->second) {
        auto it2 = indexes_.find(pr.second);
        if (it2 != indexes_.end()) {
            indexes.push_back(it2->second);
        }
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
    // 1) 查找表 ID
    auto it = table_names_.find(table_name);
    if (it == table_names_.end()) return DB_FAILED;
    cout << "DropTable (name): " << table_name << endl;
    table_id_t table_id = it->second;

    // 2) 查找表信息
    auto it2 = tables_.find(table_id);
    if (it2 == tables_.end()) return DB_FAILED;
    cout << "DropTable (id): " << table_id << endl;
    TableInfo *table_info = it2->second;

    // 3) 先删除table_name中所有的索引
    std::vector<IndexInfo *> indexes;
    if (GetTableIndexes(table_name, indexes) == DB_SUCCESS) {
        cout << "Found indexes for table: " << table_name << endl;
      for (auto index : indexes) {
        auto index_name = index->GetIndexName();
        if (DropIndex(table_name, index_name) != DB_SUCCESS) {
            return DB_FAILED;
        }
      }
    }
    else {
        cout << "No indexes found for table: " << table_name << endl;
    }

    // 4) 删除表堆
    cout << "Deleting table heap ..." << endl;
    TableHeap *table_heap = table_info->GetTableHeap();
    if (table_heap != nullptr) {
        cout << "Table heap is not null, deleting ..." << endl;
        cout << "The first page of table heap is: " << table_heap->GetFirstPageId() << endl;
        table_info->GetTableHeap()->DeleteTable(table_heap->GetFirstPageId());
    }


    // 5) 删除table_info
    cout << "Deleting table info ..." << endl;
    delete table_info;
    tables_.erase(it2);
    table_names_.erase(it);

    // 6) 删除表元数据页
    auto meta_page_it = catalog_meta_->GetTableMetaPages()->find(table_id);
    if (meta_page_it != catalog_meta_->GetTableMetaPages()->end()) {
        page_id_t page_id = meta_page_it->second;
        buffer_pool_manager_->DeletePage(page_id);
        catalog_meta_->GetTableMetaPages()->erase(meta_page_it);
    }

    // 7) 更新 catalog_meta_ 页
    FlushCatalogMetaPage();
    // 8) 返回成功
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
    // 1) 查找索引 ID
    auto it = index_names_.find(table_name);
    if (it == index_names_.end()) return DB_FAILED;
    auto it2 = it->second.find(index_name);
    if (it2 == it->second.end()) return DB_FAILED;
    index_id_t index_id = it2->second;

    // 2) 查找索引信息
    auto it3 = indexes_.find(index_id);
    if (it3 == indexes_.end()) return DB_FAILED;
    IndexInfo *index_info = it3->second;

    // 3) 删除索引信息
    delete index_info;
    indexes_.erase(it3);
    index_names_[table_name].erase(it2);

    // 4) 删除索引页
    if (!catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id)){
        return DB_FAILED;
    }

    // 5) 更新 catalog_meta_ 页
    FlushCatalogMetaPage();

    // 6) 返回成功
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
    // 1) 获取 catalog_meta_ 页
    Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    ASSERT(meta_page != nullptr, "Failed to fetch catalog meta page.");
    char *buf = meta_page->GetData();
    // 2) 序列化 catalog_meta_
    catalog_meta_->SerializeTo(buf);
    // 3) 标记脏页并释放
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, /*is_dirty=*/true);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
// 1) 取出那一页，反序列化 TableMetadata
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) return DB_FAILED;
    TableMetadata *tbl_meta = nullptr;
    TableMetadata::DeserializeFrom(page->GetData(), tbl_meta);
    buffer_pool_manager_->UnpinPage(page_id, /*is_dirty=*/false);

    // 2) 基于 metadata 和 page_id 构造 TableHeap
    TableHeap *heap = TableHeap::Create(buffer_pool_manager_,
                                        page_id,
                                        tbl_meta->GetSchema(),
                                        log_manager_,
                                        lock_manager_);

    // 3) 包装成 TableInfo 并保存到 maps
    TableInfo *tbl_info = TableInfo::Create();
    tbl_info->Init(tbl_meta, heap);
    tables_[table_id] = tbl_info;
    table_names_[tbl_meta->GetTableName()] = table_id;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
    // 1) 取出那一页，反序列化 IndexMetadata
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) return DB_FAILED;
    IndexMetadata *idx_meta = nullptr;
    IndexMetadata::DeserializeFrom(page->GetData(), idx_meta);
    buffer_pool_manager_->UnpinPage(page_id, /*is_dirty=*/false);

    // 2) 找到对应的 TableInfo
    auto it = tables_.find(idx_meta->GetTableId());
    if (it == tables_.end()) return DB_FAILED;
    TableInfo *tbl_info = it->second;

    // 3) 构造 IndexInfo 并保存到 maps
    IndexInfo *idx_info = IndexInfo::Create();
    idx_info->Init(idx_meta, tbl_info, buffer_pool_manager_);
    indexes_[index_id] = idx_info;
    index_names_[tbl_info->GetTableName()][idx_meta->GetIndexName()] = index_id;
    return DB_SUCCESS;

}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
    auto it = tables_.find(table_id);
    if (it == tables_.end()) return DB_FAILED;
    table_info = it->second;
    return DB_SUCCESS;
}