//#include <direct.h>
#include "executor/execute_engine.h"
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include "glog/logging.h"
#include <iostream>
#include <fstream>

ExecuteEngine::ExecuteEngine() {
    const std::string temp = "../../database";
    const std::string ext = ".db";
    //创建文件夹
    if(access(temp.c_str(),0) == 0){
//      cout << "exist "<< endl;
    }
    else{
      mkdir(temp.c_str(),S_IRWXG);
//      cout << "create"<< endl;
    }

    //welcome to minisql!

    cout << "welcome to our minisql" <<endl;
    // 进入目录读文件
    vector<string> file_name  = GetFiles(temp.c_str(), ext.c_str());
    // ------------------------test
    vector<string>::iterator file_name_it  ;

    if(!file_name.empty()) {
      for (file_name_it = file_name.begin(); file_name_it != file_name.end(); file_name_it++) {
        printf("%s\n",file_name_it->c_str());
        string db_name = *file_name_it + ".db";
//        string db_name = *file_name_it ;
        DBStorageEngine *storage_engine_ = new DBStorageEngine(db_name.c_str(), true, DEFAULT_BUFFER_POOL_SIZE);

        dbs_.emplace(*file_name_it, storage_engine_);
      }
    }


    for (file_name_it = file_name.begin(); file_name_it != file_name.end(); file_name_it++) {
      cout << *file_name_it << " test " << endl;
      //
    }
    // cd
        chdir("../../database");

}

/************************************************************
 * Description:
 *     获取dir目录下具有制定后缀名字的所有文件名，
 * parameter:
 *     src_dir:目录路径，例如 "../test"
 *     ext:后缀名,例如".jpg"
 * return；
 *     vector<string>：包含文件名的数组，
 * ***********************************************************/
 ///这个函数来自网上，做了一点改动
vector<string> ExecuteEngine::GetFiles(const char *src_dir, const char *ext)
{
  vector<string> result;
  string directory(src_dir);
  string m_ext(ext);

  // 打开目录, DIR是类似目录句柄的东西
  DIR *dir = opendir(src_dir);
  if ( dir == NULL )
  {
    printf("[ERROR] %s is not a directory or not exist!", src_dir);
    return result;
  }

  // dirent会存储文件的各种属性
  struct dirent* d_ent = NULL;

  // linux每个目录下面都有一个"."和".."要把这两个都去掉
  char dot[3] = ".";
  char dotdot[6] = "..";

  // 一行一行的读目录下的东西,这个东西的属性放到dirent的变量中
  while ( (d_ent = readdir(dir)) != NULL )
  {
    // 忽略 "." 和 ".."
    if ( (strcmp(d_ent->d_name, dot) != 0) && (strcmp(d_ent->d_name, dotdot) != 0) )
    {
      // d_type可以看到当前的东西的类型,DT_DIR代表当前都到的是目录,在usr/include/dirent.h中定义的
      if ( d_ent->d_type != DT_DIR)
      {
        string d_name(d_ent->d_name);
//        string d_ext =  d_name.c_str () + d_name.length ();
        if (strcmp(d_name.c_str () + d_name.length () - m_ext.length(), m_ext.c_str ()) == 0  )
        {
          string file_name = string(d_ent->d_name);
          int size = file_name.length();
          file_name.resize(size - m_ext.length());
          file_name.resize(size );
          result.push_back(file_name );
        }
      }
    }
  }

  // sort the returned files
  sort(result.begin(), result.end());

  closedir(dir);
  return result;
}


dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  printf("Execute prepare\n");
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  if(ast->child_->type_ == kNodeIdentifier){
    pSyntaxNode db_name_node = ast->child_;
    std::string db_name = string (db_name_node->val_);

    // create db file
    ofstream File;
    string  db_name_db = db_name +".db";
//    File.open( db_name,ios::out|ios::app);
//    File.close();

    DBStorageEngine *storage_engine_ = new DBStorageEngine(db_name_db.c_str(), true, DEFAULT_BUFFER_POOL_SIZE);
    dbs_.emplace(db_name, storage_engine_);
    printf("create success!\n");
    return  DB_SUCCESS;
  }


  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  pSyntaxNode db_name_node = ast->child_;
  std::string db_name = string (db_name_node->val_);
  if(dbs_.find(db_name)!=dbs_.end()){
    dbs_.erase(db_name);
//    if(current_db_ == db_name){
//      current_db_ == dbs_.begin()->first;
//    }
    return  DB_SUCCESS;
  }
  printf("no such database\n");
  return  DB_FAILED;

}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  unordered_map<std::string, DBStorageEngine *>::iterator dbs_it  ;
  if(dbs_.empty()){
    printf("no database\n");
  }

  if(!dbs_.empty()) {
    printf("|----------------------|\n");
    printf("|  show databases      |\n");
    printf("|----------------------|\n");
    for (dbs_it  = dbs_.begin(); dbs_it  != dbs_.end(); dbs_it ++) {
      string db_name = dbs_it->first;
      printf("| ");
      printf("%s", db_name.c_str());
      for(int i=20-db_name.length();i>=0; i--){
        printf(" ");
      }
      printf("|\n");
    }
    printf("|--------------------- |\n");
  }



  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  pSyntaxNode db_name_node = ast->child_;
  std::string db_name = string (db_name_node->val_);
  if(dbs_.find(db_name)!=dbs_.end()){
    current_db_ = db_name;
    return  DB_SUCCESS;
  }
  else{
    printf("no such database");
  }
  return  DB_FAILED;

}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  printf("|----------------------|\n");
  printf("|  show tables         |\n");
  printf("|----------------------|\n");
  vector<TableInfo* > tables_now;
  DBStorageEngine * current_db_engine = dbs_[current_db_];
  current_db_engine->catalog_mgr_->GetTables(tables_now);
  for(auto table_it=tables_now.begin();table_it<tables_now.end();table_it++){
    string table_name = (*table_it)->GetTableName();
    printf("| ");
    printf("%s", table_name.c_str());
    for(int i=20-table_name.length();i>=0; i--){
      printf(" ");
    }
    printf("|\n");
  }
  return DB_SUCCESS;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  DBStorageEngine * current_db_engine = dbs_[current_db_];
  pSyntaxNode table_name_node = ast->child_;
  string table_name = table_name_node->val_;

  vector<Column*>column_;
  pSyntaxNode column_attribute_now = table_name_node->next_->child_;

  while( column_attribute_now->next_ != nullptr && column_attribute_now->type_==kNodeColumnDefinition ){
    // is unique?
    bool  unique = false;
    string is_unique = column_attribute_now->val_;
    if(is_unique == "unique"){
      unique = true;
    }
    string column_name = column_attribute_now->child_->val_;
    string column_type = column_attribute_now->child_->next_->val_;
    int index=0;
    Column * now_column;
    if( column_type == "int"){
      now_column = new Column(column_name,kTypeInt, index, true, unique);
    }
    else if(column_type=="char"){

      uint32_t length = atoi(column_attribute_now->child_->next_->child_->val_);
      now_column = new Column(column_name,kTypeChar,length, index, true, unique);
    }
    else if(column_type=="float"){

      now_column = new Column(column_name,kTypeFloat,index,true,unique);
    }
    else{
      printf("no such type!\n");
      return DB_FAILED;
    }
    column_.push_back(now_column);
    column_attribute_now = column_attribute_now->next_;
    index++;


  }
  Schema *schema_now = new Schema(column_);
  TableInfo *table_info = nullptr;
  //cout<<"SUCCEED!"<<endl;
  dberr_t Created = current_db_engine->catalog_mgr_->CreateTable(table_name,schema_now,nullptr,table_info);
  if(Created == DB_TABLE_ALREADY_EXIST){
    printf("db table already exist\n");
    return DB_TABLE_ALREADY_EXIST;
  }
  // get pk
  if(column_attribute_now!= nullptr && column_attribute_now->type_ == kNodeColumnList){
    pSyntaxNode PK_node = column_attribute_now->child_;
    vector <string>PK;
    while(PK_node!=nullptr){
      string PK_name = PK_node->val_ ;
      PK.push_back(PK_name);
      PK_node = PK_node->next_;
    }
    CatalogManager* current_catalog = current_db_engine->catalog_mgr_;
    IndexInfo* index_info=nullptr;

    current_catalog->CreateIndex(table_name,table_name+"_PK",PK,nullptr,index_info);

  }

  return DB_TABLE_NOT_EXIST;
//  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
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

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
