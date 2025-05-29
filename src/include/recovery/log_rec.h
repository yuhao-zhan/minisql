#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>
#include <memory>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};
    
    // 用于存储操作相关的数据
    KeyType key_;
    ValType val_;
    KeyType new_key_;  // 用于更新操作
    ValType new_val_;  // 用于更新操作

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

// 静态成员变量的定义
inline std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
inline lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

// 静态函数定义
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    auto log_record = std::make_shared<LogRec>();
    log_record->type_ = LogRecType::kInsert;
    log_record->txn_id_ = txn_id;
    log_record->key_ = ins_key;
    log_record->val_ = ins_val;
    
    // 设置LSN
    log_record->lsn_ = LogRec::next_lsn_++;
    // 设置prev_lsn
    log_record->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    // 更新prev_lsn_map
    LogRec::prev_lsn_map_[txn_id] = log_record->lsn_;
    
    return log_record;
}

static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    auto log_record = std::make_shared<LogRec>();
    log_record->type_ = LogRecType::kDelete;
    log_record->txn_id_ = txn_id;
    log_record->key_ = del_key;
    log_record->val_ = del_val;
    
    log_record->lsn_ = LogRec::next_lsn_++;
    log_record->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = log_record->lsn_;
    
    return log_record;
}

static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    auto log_record = std::make_shared<LogRec>();
    log_record->type_ = LogRecType::kUpdate;
    log_record->txn_id_ = txn_id;
    log_record->key_ = old_key;
    log_record->val_ = old_val;
    log_record->new_key_ = new_key;
    log_record->new_val_ = new_val;
    
    log_record->lsn_ = LogRec::next_lsn_++;
    log_record->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = log_record->lsn_;
    
    return log_record;
}

static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    auto log_record = std::make_shared<LogRec>();
    log_record->type_ = LogRecType::kBegin;
    log_record->txn_id_ = txn_id;
    
    log_record->lsn_ = LogRec::next_lsn_++;
    log_record->prev_lsn_ = INVALID_LSN;  // 事务开始时没有前序日志
    LogRec::prev_lsn_map_[txn_id] = log_record->lsn_;
    
    return log_record;
}

static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    auto log_record = std::make_shared<LogRec>();
    log_record->type_ = LogRecType::kCommit;
    log_record->txn_id_ = txn_id;
    
    log_record->lsn_ = LogRec::next_lsn_++;
    log_record->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = log_record->lsn_;
    
    return log_record;
}

static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    auto log_record = std::make_shared<LogRec>();
    log_record->type_ = LogRecType::kAbort;
    log_record->txn_id_ = txn_id;
    
    log_record->lsn_ = LogRec::next_lsn_++;
    log_record->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = log_record->lsn_;
    
    return log_record;
}

#endif  // MINISQL_LOG_REC_H
