#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * TODO: Student Implement
    */
    void Init(CheckPoint &last_checkpoint) {
        // 初始化数据库状态
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
        active_txns_ = last_checkpoint.active_txns_;
        data_ = last_checkpoint.persist_data_;
    }

    /**
    * TODO: Student Implement
    */
    void RedoPhase() {
        // 重做阶段：从最早的LSN开始，按顺序重放所有日志记录
        for (const auto &[lsn, log_rec] : log_recs_) {
            if (lsn > persist_lsn_) {
                // 只重做未持久化的操作
                switch (log_rec->type_) {
                    case LogRecType::kInsert:
                        data_[log_rec->key_] = log_rec->val_;
                        break;
                    case LogRecType::kDelete:
                        data_.erase(log_rec->key_);
                        break;
                    case LogRecType::kUpdate:
                        data_[log_rec->new_key_] = log_rec->new_val_;
                        break;
                    case LogRecType::kCommit:
                    case LogRecType::kAbort:
                        active_txns_.erase(log_rec->txn_id_);
                        break;
                    case LogRecType::kBegin:
                        active_txns_[log_rec->txn_id_] = log_rec->lsn_;
                        break;
                    default:
                        break;
                }
                // 更新最后持久化的LSN
                persist_lsn_ = lsn;
            }
        }
    }

    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
        // 撤销阶段：处理所有未完成的事务
        std::map<lsn_t, LogRecPtr> undo_actions;
        
        // 收集所有需要撤销的事务的操作
        for (const auto &[txn_id, last_lsn] : active_txns_) {
            for (const auto &[lsn, log_rec] : log_recs_) {
                if (log_rec->txn_id_ == txn_id) {
                    undo_actions[lsn] = log_rec;
                }
            }
        }

        // 从最新的LSN开始，逆序撤销操作
        for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it) {
            const auto &log_rec = it->second;
            
            switch (log_rec->type_) {
                case LogRecType::kInsert:
                    // 撤销插入操作就是删除
                    data_.erase(log_rec->key_);
                    break;
                case LogRecType::kDelete:
                    // 撤销删除操作就是插入原值
                    data_[log_rec->key_] = log_rec->val_;
                    break;
                case LogRecType::kUpdate:
                    // 撤销更新操作就是恢复原值
                    data_[log_rec->key_] = log_rec->val_;
                    break;
                default:
                    break;
            }
        }

        // 清空活跃事务表
        active_txns_.clear();
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
