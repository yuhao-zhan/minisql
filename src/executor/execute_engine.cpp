#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>
#include <filesystem>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
    char path[] = "./databases";
    DIR *dir;
    if ((dir = opendir(path)) == nullptr) {
        mkdir("./databases", 0777);
        dir = opendir(path);
    }
    /** When you have completed all the code for
     *  the test, run it using main.cpp and uncomment
     *  this part of the code.
    struct dirent *stdir;
    while((stdir = readdir(dir)) != nullptr) {
      if( strcmp( stdir->d_name , "." ) == 0 ||
          strcmp( stdir->d_name , "..") == 0 ||
          stdir->d_name[0] == '.')
        continue;
      dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
    }
     **/
    closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
    switch (plan->GetType()) {
        // Create a new sequential scan executor
        case PlanType::SeqScan: {
            return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
        }
            // Create a new index scan executor
        case PlanType::IndexScan: {
            return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
        }
            // Create a new update executor
        case PlanType::Update: {
            auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
            return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
        }
            // Create a new delete executor
        case PlanType::Delete: {
            auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
            return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
        }
        case PlanType::Insert: {
            auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
            return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
        }
        case PlanType::Values: {
            return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
        }
        default:
            throw std::logic_error("Unsupported plan tyNextpe.");
    }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
    // Construct the executor for the abstract plan node
    auto executor = CreateExecutor(exec_ctx, plan);

    try {
        executor->Init();
        RowId rid{};
        Row row{};
        while (executor->Next(&row, &rid)) {
            if (result_set != nullptr) {
                result_set->push_back(row);
            }
        }
    } catch (const exception &ex) {
        // std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
        if (result_set != nullptr) {
            result_set->clear();
        }
        return DB_FAILED;
    }
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
    if (ast == nullptr) {
        return DB_FAILED;
    }
    auto start_time = std::chrono::system_clock::now();
    unique_ptr<ExecuteContext> context(nullptr);
    if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
    switch (ast->type_) {
        case kNodeCreateDB:
            return ExecuteCreateDatabase(ast, context.get());
        case kNodeDropDB:
            return ExecuteDropDatabase(ast, context.get());
        case kNodeShowDB:
            return ExecuteShowDatabases(ast, context.get());
        case kNodeUseDB:
            return ExecuteUseDatabase(ast, context.get());
        case kNodeShowTables:
            return ExecuteShowTables(ast, context.get());
        case kNodeCreateTable:
            return ExecuteCreateTable(ast, context.get());
        case kNodeDropTable:
            return ExecuteDropTable(ast, context.get());
        case kNodeShowIndexes:
            return ExecuteShowIndexes(ast, context.get());
        case kNodeCreateIndex:
            return ExecuteCreateIndex(ast, context.get());
        case kNodeDropIndex:
            return ExecuteDropIndex(ast, context.get());
        case kNodeTrxBegin:
            return ExecuteTrxBegin(ast, context.get());
        case kNodeTrxCommit:
            return ExecuteTrxCommit(ast, context.get());
        case kNodeTrxRollback:
            return ExecuteTrxRollback(ast, context.get());
        case kNodeExecFile:
            return ExecuteExecfile(ast, context.get());
        case kNodeQuit:
            return ExecuteQuit(ast, context.get());
        default:
            break;
    }
    // Plan the query.
    Planner planner(context.get());
    std::vector<Row> result_set{};
    try {
        planner.PlanQuery(ast);
        // Execute the query.
        ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
    } catch (const exception &ex) {
        // // std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
        return DB_FAILED;
    }
    auto stop_time = std::chrono::system_clock::now();
    double duration_time =
            double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
    // Return the result set as string.
    std::stringstream ss;
    ResultWriter writer(ss);

    if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
        auto schema = planner.plan_->OutputSchema();
        auto num_of_columns = schema->GetColumnCount();
        if (!result_set.empty()) {
            // find the max width for each column
            vector<int> data_width(num_of_columns, 0);
            for (const auto &row : result_set) {
                for (uint32_t i = 0; i < num_of_columns; i++) {
                    data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
                }
            }
            int k = 0;
            for (const auto &column : schema->GetColumns()) {
                data_width[k] = max(data_width[k], int(column->GetName().length()));
                k++;
            }
            // Generate header for the result set.
            writer.Divider(data_width);
            k = 0;
            writer.BeginRow();
            for (const auto &column : schema->GetColumns()) {
                writer.WriteHeaderCell(column->GetName(), data_width[k++]);
            }
            writer.EndRow();
            writer.Divider(data_width);

            // Transforming result set into strings.
            for (const auto &row : result_set) {
                writer.BeginRow();
                for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
                    writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
                }
                writer.EndRow();
            }
            writer.Divider(data_width);
        }
        writer.EndInformation(result_set.size(), duration_time, true);
    } else {
        writer.EndInformation(result_set.size(), duration_time, false);
    }
    std::cout << writer.stream_.rdbuf();
    // todo:: use shared_ptr for schema
    if (ast->type_ == kNodeSelect)
        delete planner.plan_->OutputSchema();
    return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
    switch (result) {
        case DB_ALREADY_EXIST:
            cout << "Database already exists." << endl;
            break;
        case DB_NOT_EXIST:
            cout << "Database not exists." << endl;
            break;
        case DB_TABLE_ALREADY_EXIST:
            cout << "Table already exists." << endl;
            break;
        case DB_TABLE_NOT_EXIST:
            cout << "Table not exists." << endl;
            break;
        case DB_INDEX_ALREADY_EXIST:
            cout << "Index already exists." << endl;
            break;
        case DB_INDEX_NOT_FOUND:
            cout << "Index not exists." << endl;
            break;
        case DB_COLUMN_NAME_NOT_EXIST:
            cout << "Column not exists." << endl;
            break;
        case DB_KEY_NOT_FOUND:
            cout << "Key not exists." << endl;
            break;
        case DB_QUIT:
            cout << "Bye." << endl;
            break;
        default:
            break;
    }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
    string db_name = ast->child_->val_;
    if (dbs_.find(db_name) != dbs_.end()) {
        return DB_ALREADY_EXIST;
    }
    dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
    string db_name = ast->child_->val_;
    if (dbs_.find(db_name) == dbs_.end()) {
        return DB_NOT_EXIST;
    }
    remove(("./databases/" + db_name).c_str());
    delete dbs_[db_name];
    dbs_.erase(db_name);
    if (db_name == current_db_)
        current_db_ = "";
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
    if (dbs_.empty()) {
        cout << "Empty set (0.00 sec)" << endl;
        return DB_SUCCESS;
    }
    int max_width = 8;
    for (const auto &itr : dbs_) {
        if (itr.first.length() > max_width) max_width = itr.first.length();
    }
    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
         << " |" << endl;
    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    for (const auto &itr : dbs_) {
        cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
    }
    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
    string db_name = ast->child_->val_;
    if (dbs_.find(db_name) != dbs_.end()) {
        current_db_ = db_name;
        cout << "Database changed" << endl;
        return DB_SUCCESS;
    }
    return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
    if (current_db_.empty()) {
        cout << "No database selected" << endl;
        return DB_FAILED;
    }
    vector<TableInfo *> tables;
    if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
        cout << "Empty set (0.00 sec)" << endl;
        return DB_FAILED;
    }
    string table_in_db("Tables_in_" + current_db_);
    uint max_width = table_in_db.length();
    for (const auto &itr : tables) {
        if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
    }
    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    for (const auto &itr : tables) {
        cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
    }
    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
    if (current_db_.empty()) {
        // std::cout << "No database selected" << std::endl;
        return DB_FAILED;
    }

    string table_name = ast->child_->val_;
    TableInfo * table_info;
    if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_SUCCESS) {
        return DB_TABLE_ALREADY_EXIST;
    }

    std::vector<Column *> columns;
    std::vector<string> column_names;
    std::vector<string> column_type_strs;
    std::vector<uint32_t> lengths;
    std::vector<bool> uniques;
    std::string primary_name;

    auto column_definition_list = ast->child_->next_;

    for (auto column = column_definition_list->child_; column != nullptr; column = column->next_) {

        if (column->type_ == kNodeColumnDefinition) {
            column_names.push_back(column->child_->val_);
            column_type_strs.push_back(column->child_->next_->val_);

            if (column_type_strs.back() == "char") {
                lengths.push_back(std::stoi(column->child_->next_->child_->val_));
                if (lengths.back() <= 0) {
                    std::cerr << "Invalid Length" << std::endl;
                    return DB_FAILED;
                }
            } else {
                lengths.push_back(0);
            }

            string unique = "unique";
            if (column->val_ != nullptr && column->val_ == unique) {
                uniques.push_back(1);
            } else {
                uniques.push_back(0);
            }

        } else if (column->type_ == kNodeColumnList) {
            for (auto primary_key = column->child_; primary_key != nullptr; primary_key = primary_key->next_) {
                primary_name = primary_key->val_;
            }
        } else {
            std::cerr << "Invaild SyntaxNode" << std::endl;
            return DB_FAILED;
        }
    }

    for (auto i = 0; i < column_names.size(); i++) {

        bool nullable = true;
        if (primary_name == column_names[i]) {
            uniques[i] = 1;
            nullable = false;
        }

        Column *pcolumn;
        if (column_type_strs[i] == "int") {
            pcolumn = new Column(column_names[i], kTypeInt, i, nullable, uniques[i]);
        } else if (column_type_strs[i] == "float") {
            pcolumn = new Column(column_names[i], kTypeFloat, i, nullable, uniques[i]);
        } else if (column_type_strs[i] == "char") {
            pcolumn = new Column(column_names[i], kTypeChar, lengths[i], i, nullable, uniques[i]);
        } else {
            std::cerr << "Invalid Type" << std::endl;
            return DB_FAILED;
        }
        columns.push_back(pcolumn);
    }

    TableSchema * table_schema = new TableSchema(columns);
    Txn * txn;
    dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, table_schema, txn, table_info);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
    if (current_db_.empty()) {
        // std::cout << "No database selected" << std::endl;
        return DB_FAILED;
    }

    string table_name = ast->child_->val_;
    TableInfo * table_info;
    if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_SUCCESS) {
        dbs_[current_db_]->catalog_mgr_->DropTable(ast->child_->val_);
    } else {
        // std::cout << "Table not exists" << std::endl;
        return DB_FAILED;
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
    if (current_db_.empty()) {
        cout << "No database selected" << endl;
        return DB_FAILED;
    }

    vector<TableInfo *> tables;
    if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
        cout << "Empty set (0.00 sec)" << endl;
        return DB_FAILED;
    }

    vector<IndexInfo *> indexes;
    for (const auto& table : tables) {
        auto table_name = table->GetTableName();
        dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
    }

    string index_in_db("Indexes_in_" + current_db_);
    uint max_width = index_in_db.length();

    for (const auto &itr : indexes) {
        if (itr->GetIndexName().length() > max_width) max_width = itr->GetIndexName().length();
    }

    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    cout << "| " << std::left << setfill(' ') << setw(max_width) << index_in_db << " |" << endl;
    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    for (const auto &itr : indexes) {
        cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetIndexName() << " |" << endl;
    }
    cout << "+" << setfill('-') << setw(max_width + 2) << ""
         << "+" << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
    if (current_db_.empty()) {
        cout << "No database selected" << endl;
        return DB_FAILED;
    }

    string index_name = ast->child_->val_;
    string table_name = ast->child_->next_->val_;
    auto keys_node = ast->child_->next_->next_;
    vector<string> keys;
    for (auto column = keys_node->child_; column != nullptr; column = column->next_) {
        keys.push_back(column->val_);
    }

    IndexInfo * index_info;
    string index_type;
    dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, keys, nullptr, index_info, index_type);
    // std::cout << "Index Created Successfully!" << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
    if (current_db_.empty()) {
        cout << "No database selected" << endl;
        return DB_FAILED;
    }

    string index_name = ast->child_->val_;

    vector<TableInfo* > tables;
    dbs_[current_db_]->catalog_mgr_->GetTables(tables);

    for (auto table: tables) {
        if (dbs_[current_db_]->catalog_mgr_->DropIndex(table->GetTableName(), index_name) == DB_SUCCESS){
            // std::cout << "Index Dropped Successfully" << std::endl;
            return DB_SUCCESS;
        }
    }

    // std::cout << "No Such Index" << std::endl;
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
    return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
    string filename = ast->child_->val_;
    if (!std::filesystem::exists(filename)) {
        LOG(ERROR) << "File not found: " << filename << std::endl;
        return DB_FAILED;
    }

    std::ifstream file(filename);
    if (!file.is_open()) return DB_FAILED;

    const int buf_size = 1024;
    char cmd[buf_size];

    while (!file.eof()) {
        memset(cmd, 0, buf_size);
        int i = 0;
        char ch;
        while (!file.eof() && (ch=file.get()) != ';') {
            cmd[i++] = ch;
        }
        cmd[i] = ch;
        file.get();

        YY_BUFFER_STATE bp = yy_scan_string(cmd);

        if (bp == nullptr) {
            LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
            exit(1);
        }
        yy_switch_to_buffer(bp);

        // init parser module
        MinisqlParserInit();

        // parse
        yyparse();

        // parse result handle
        if (MinisqlParserGetError()) {
            // error
            printf("%s\n", MinisqlParserGetErrorMessage());
        } else {
            // Comment them out if you don't need to debug the syntax tree
            printf("[INFO] Sql syntax parse ok!\n");
        }

        cout << "Start to execute sql..." << endl;
        auto result = Execute(MinisqlGetParserRootNode());

        // clean memory after parse
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();

        // quit condition
        ExecuteInformation(result);
        if (result == DB_QUIT) {
            break;
        }
    }

    // std::cout << "File Executed Successfully" << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
    current_db_.clear();
    std:: cout << "Bye" << std::endl;
    exit(0);
    return DB_SUCCESS;
}