#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/config.h"
#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> latch(latch_);
    
    // 准备加锁前的检查
    LockPrepare(txn, rid);
    
    auto &req_queue = lock_table_[rid];
    req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
    
    auto it = req_queue.GetLockRequestIter(txn->GetTxnId());
    LockRequest &req = *it;

    txn_id_t txn_id = txn->GetTxnId();

    if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted && req.lock_mode_ == LockMode::kShared) {
        req_queue.EraseLockRequest(txn_id);
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn_id, AbortReason::kLockSharedOnReadUncommitted);
    }

    while (true) {
        if (!req_queue.is_writing_ && !req_queue.is_upgrading_) {
            req_queue.sharing_cnt_++;
            req.granted_ = LockMode::kShared;
            txn->GetSharedLockSet().insert(rid);
            for (auto &entry: req_queue.req_list_) {
                if (entry.txn_id_ != txn->GetTxnId()) {
                    RemoveEdge(txn->GetTxnId(), entry.txn_id_);
                }
            }
            return true;
        }

        if (req_queue.is_writing_) {
            for (auto &entry: req_queue.req_list_) {
                if (entry.granted_ == LockMode::kExclusive) {
                    AddEdge(txn->GetTxnId(),  entry.txn_id_);
                }
            }
        }

        CheckAbort(txn, req_queue);
        req_queue.cv_.wait(latch);
    }
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> latch(latch_);
    txn_id_t txn_id = txn->GetTxnId();

    LockPrepare(txn, rid);

    auto &req_queue = lock_table_[rid];
    req_queue.EmplaceLockRequest(txn_id, LockMode::kExclusive);

    auto it = req_queue.GetLockRequestIter(txn_id);
    LockRequest &req = *it;

    while (true) {
        if (!req_queue.is_writing_ && !req_queue.is_upgrading_ && req_queue.sharing_cnt_ == 0) {
            req_queue.is_writing_ = true;
            req.granted_ = LockMode::kExclusive;
            txn->GetExclusiveLockSet().insert(rid);
            for (auto &entry: req_queue.req_list_) {
                if (entry.txn_id_ != txn_id) {
                    RemoveEdge(txn_id, entry.txn_id_);
                }
            }
            return true;
        }

        if (req_queue.is_writing_) {
            for (auto &entry: req_queue.req_list_) {
                if (entry.granted_ == LockMode::kExclusive) {
                    AddEdge(txn_id, entry.txn_id_);
                }
            }
        }

        if (req_queue.sharing_cnt_ > 0) {
            for (auto &entry: req_queue.req_list_) {
                if (entry.granted_ == LockMode::kShared) {
                    AddEdge(txn_id, entry.txn_id_);
                }
            }
        }

        CheckAbort(txn, req_queue);
        req_queue.cv_.wait(latch);
    }
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> latch(latch_);
    txn_id_t txn_id = txn->GetTxnId();

    LockPrepare(txn, rid);

    if (txn->GetState() != TxnState::kGrowing) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn_id, AbortReason::kDeadlock);
        return false;
    }

    auto &req_queue = lock_table_[rid];

    auto it = req_queue.GetLockRequestIter(txn_id);
    LockRequest &req = *it;

    if (req_queue.is_upgrading_) {
        req_queue.sharing_cnt_--;  // 减少共享计数
        txn->GetSharedLockSet().erase(rid);  // 从事务锁集合中移除
        req_queue.EraseLockRequest(txn_id);
        txn->SetState(TxnState::kAborted);
        req_queue.cv_.notify_all();
        throw TxnAbortException(txn_id, AbortReason::kUpgradeConflict);
    }

    req_queue.is_upgrading_ = true;

    while (true) {
        if (txn->GetState() == TxnState::kAborted) {
          req_queue.EraseLockRequest(txn_id);
          req_queue.is_upgrading_ = false;
          req_queue.cv_.notify_all();
          throw TxnAbortException(txn_id, AbortReason::kDeadlock);
          return false;
        }

        if (!req_queue.is_writing_ && req_queue.sharing_cnt_ == 1) {
            req_queue.is_writing_ = true;
            req_queue.sharing_cnt_--;  // 读者数量减一
            txn->GetSharedLockSet().erase(rid);

            req.lock_mode_ = LockMode::kExclusive;
            req.granted_ = LockMode::kExclusive;
            txn->GetExclusiveLockSet().insert(rid);
            req_queue.is_upgrading_ = false;

            for (auto &entry : req_queue.req_list_) {
                if (entry.txn_id_ != txn_id) {
                    RemoveEdge(txn_id, entry.txn_id_);
                }
            }
            return true;
        }

        if (req_queue.is_writing_) {
            for (auto & entry : req_queue.req_list_) {
                if (entry.granted_ == LockMode::kExclusive) {
                    AddEdge(txn_id, entry.txn_id_);
                }
            }
        }

        if (req_queue.sharing_cnt_ > 0) {
            for (auto & entry : req_queue.req_list_) {
                if (entry.granted_ == LockMode::kShared) {
                    AddEdge(txn_id, entry.txn_id_);
                }
            }
        }

        CheckAbort(txn, req_queue);

        req_queue.cv_.wait(latch);
    }
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lk(latch_);
    txn_id_t tid = txn->GetTxnId();

    // 1. 找到对应的请求队列
    auto queue_it = lock_table_.find(rid);
    if (queue_it == lock_table_.end()) {
        return false;  // 根本不存在该行的任何请求
    }

    auto &req_queue = queue_it->second;

    // 2. 找到本事务在队列中的请求
    auto iter = req_queue.req_list_iter_map_.find(tid);
    if (iter == req_queue.req_list_iter_map_.end()) {
        return false;  // 该事务未对该行申请过锁
    }

    LockRequest &request = *iter->second;

    // 3. 如果还没被授予任何锁，直接返回
    if (request.granted_ == LockMode::kNone) {
        return false;
    }

    // 4. 第一次释放锁时，事务进入收缩期（Shrinking）
    if (txn->GetState() == TxnState::kGrowing) {
        txn->SetState(TxnState::kShrinking);
    }

    // 5. 根据锁类型，更新队列状态和事务的锁集合
    if (request.granted_ == LockMode::kShared) {
        // 释放一个共享锁
        req_queue.sharing_cnt_--;
        txn->GetSharedLockSet().erase(rid);
    } else if (request.granted_ == LockMode::kExclusive) {
        // 释放排他锁
        req_queue.is_writing_ = false;
        txn->GetExclusiveLockSet().erase(rid);
    }

    // 6. 从请求队列中移除该请求
    req_queue.EraseLockRequest(tid);

    // 7. 唤醒所有在该行上等待的事务，重新尝试获取锁
    req_queue.cv_.notify_all();

    return true;    
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
    if (txn->GetState() == TxnState::kShrinking) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockRequestQueue &req_queue) {
    if (txn->GetState() == TxnState::kAborted) {
        req_queue.EraseLockRequest(txn->GetTxnId());
        DeleteNode(txn->GetTxnId());
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    // 避免自环
    if (t1 == t2) return;
    // 在 waits-for 图中为 t1 添加一条指向 t2 的依赖边
    waits_for_[t1].insert(t2);

    // 确保 t2 也作为图中的一个节点（即使它暂时没有出边）
    if (waits_for_.find(t2) == waits_for_.end()) {
        waits_for_.emplace(t2, std::set<txn_id_t>{});
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    auto it = waits_for_.find(t1);
    if (it == waits_for_.end()) {
        return;  // t1 本身就不在图中
    }
    // 从 t1 的依赖集合中移除 t2
    it->second.erase(t2);
    // 如果 t1 已无任何依赖，则从图中彻底删除这个节点
    if (it->second.empty()) {
        waits_for_.erase(it);
    }
}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
    // 1. 清理上次搜索状态
    visited_set_.clear();
    visited_path_ = std::stack<txn_id_t>();
    revisited_node_ = INVALID_TXN_ID;

    // 2. 递归 DFS 函数：返回 true 表示在 node 的子图中发现了环
    std::function<bool(txn_id_t)> dfs = [&](txn_id_t node) -> bool {
        // 如果 node 已在当前路径中，找到了环
        if (visited_set_.count(node)) {
            revisited_node_ = node;
            return true;
        }
        // 标记进入当前路径
        visited_set_.insert(node);
        visited_path_.push(node);

        // 遍历 node 的出边
        auto it = waits_for_.find(node);
        if (it != waits_for_.end()) {
            for (txn_id_t neigh : it->second) {
                if (dfs(neigh)) {
                    return true;
                }
            }
        }

        // 回溯：从当前路径中移除
        visited_set_.erase(node);
        visited_path_.pop();
        return false;
    };

    // 3. 对图中每个节点启动 DFS（直到找到第一个环）
    for (auto &entry : waits_for_) {
        if (dfs(entry.first)) {
            break;
        }
    }

    // 4. 如果没发现环，直接返回 false
    if (revisited_node_ == INVALID_TXN_ID) {
        return false;
    }

    // 5. 从 visited_path_ 中收集环节点（从栈顶到 revisited_node_ 为止）
    std::stack<txn_id_t> temp = visited_path_;
    std::vector<txn_id_t> cycle_nodes;
    while (!temp.empty()) {
        txn_id_t t = temp.top(); 
        temp.pop();
        cycle_nodes.push_back(t);
        if (t == revisited_node_) break;
    }

    // 6. 在环节点中选出最大的 txn_id 作为“最年轻”的事务
    txn_id_t youngest = cycle_nodes.front();
    for (txn_id_t id : cycle_nodes) {
        if (id > youngest) youngest = id;
    }
    newest_tid_in_cycle = youngest;

    return true;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id: txn->GetSharedLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }

    for (const auto &row_id: txn->GetExclusiveLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::RunCycleDetection() {
   cycle_detection_thread_ = std::thread([this]() {
        while (enable_cycle_detection_) {
            std::this_thread::sleep_for(cycle_detection_interval_);
            txn_id_t victim_tid = INVALID_TXN_ID;
            {
                std::lock_guard<std::mutex> guard(latch_);
                if (!HasCycle(victim_tid)) continue;
                DeleteNode(victim_tid);
            }
            if (txn_mgr_) {
                if (auto *victim = txn_mgr_->GetTransaction(victim_tid)) {
                    txn_mgr_->Abort(victim);
                }
            }
        }
    }); 
}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;
    for (auto &entry: waits_for_) {
        for (auto &neigh: entry.second) {
            result.emplace_back(entry.first, neigh);
        }
    }
    return result;
}
