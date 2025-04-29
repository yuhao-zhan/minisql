#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"

class TableHeap;

class TableIterator {
public:
    // you may define your own constructor based on your member variables
    explicit TableIterator(TableHeap *table_heap, RowId rid, Txn *txn);

    TableIterator(const TableIterator &other);

    virtual ~TableIterator();

    bool operator==(const TableIterator &itr) const;

    bool operator!=(const TableIterator &itr) const;

    const Row &operator*();

    Row *operator->();

    TableIterator &operator=(const TableIterator &itr) noexcept;

    TableIterator &operator++();

    TableIterator operator++(int);

private:
    // add your own private member variables here
    TableHeap *table_heap_;  // 所属表堆
    RowId rid_;              // 当前行的 RowId
    Txn *txn_;               // 事务上下文

    Row cur_row_;            // 存放当前行内容
};

#endif  // MINISQL_TABLE_ITERATOR_H
