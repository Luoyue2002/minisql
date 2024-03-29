//#include <direct.h>
#include "executor/execute_engine.h"
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include "glog/logging.h"
#include <iostream>
#include <iomanip>
#include <fstream>

extern "C" {
int yyparse(void);
//FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
ExecuteEngine::ExecuteEngine() {
    const std::string temp = "database";
    const std::string temp_file = "testfile";
    const std::string ext = ".db";
    //创建文件夹
    if(access(temp.c_str(),0) == 0){
//      cout << "exist "<< endl;
    }
    else{
      mkdir(temp.c_str(),S_IRWXG);
//      cout << "create"<< endl;
    }

//    if(access(temp_file.c_str(),0) == 0){
//      //      cout << "exist "<< endl;
//    }
//    else{
//      mkdir(temp_file.c_str(),S_IRWXG);
//      //      cout << "create"<< endl;
//    }

    //welcome to minisql!

    // cout << "welcome to our minisql" <<endl;
    // 进入目录读文件
    vector<string> file_name  = GetFiles(temp.c_str(), ext.c_str());
    // ------------------------test
    vector<string>::iterator file_name_it  ;

    // change the cd dir there, to make new DBStorageEngine Work in the path
    chdir("database");

    if(!file_name.empty()) {
      for (file_name_it = file_name.begin(); file_name_it != file_name.end(); file_name_it++) {
        //printf("%s\n",file_name_it->c_str());
        string db_name = *file_name_it + ".db";
//        string db_name = *file_name_it ;
        DBStorageEngine *storage_engine_ = new DBStorageEngine(db_name.c_str(), false, DEFAULT_BUFFER_POOL_SIZE);

        dbs_.emplace(*file_name_it, storage_engine_);
      }
    }


    for (file_name_it = file_name.begin(); file_name_it != file_name.end(); file_name_it++) {
      // cout << *file_name_it << " test " << endl;
      //
    }

    current_db_engine = NULL;

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
//          file_name.resize(size );
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
  // printf("Execute prepare\n");
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

// ======================== helping function ======================== //

static void PrintNode(pSyntaxNode ast, int level) {
  if(ast == NULL) return;
  printf("level: %d, %s\n", level , ast->val_);
  PrintNode(ast->child_, level+1);
  PrintNode(ast->next_, level);
}

// ======================== helping function ======================== //

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
    printf("Create success!\n");
    cout << "\033[32m[Rows]\033[0m " << "1 row affected. " << endl;
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
    remove((db_name+".db").c_str());
//    if(current_db_ == db_name){
//      current_db_ == dbs_.begin()->first;
//    }
    cout << "Drop success!\n" << endl;
    cout << "\033[32m[Rows]\033[0m " << "1 row affected. " << endl;
    return  DB_SUCCESS;
  }
  printf("No such database!\n");
  return  DB_FAILED;

}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  unordered_map<std::string, DBStorageEngine *>::iterator dbs_it  ;
  if(dbs_.empty()){
    printf("No database.\n");
  }

  int num = 0;
  if(!dbs_.empty()) {
    printf("+----------------------+\n");
    printf("| Database             |\n");
    printf("+----------------------+\n");
    for (dbs_it  = dbs_.begin(); dbs_it  != dbs_.end(); dbs_it ++) {
      string db_name = dbs_it->first;
      printf("| ");
      printf("%s", db_name.c_str());
      for(int i=20-db_name.length();i>=0; i--){
        printf(" ");
      }
      printf("|\n");
      num ++;
    }
    printf("+----------------------+\n");
  }
  cout << "\033[32m[Rows]\033[0m " << num << " rows affected. " << endl;

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

      unordered_map<std::string, DBStorageEngine *>::iterator dbs_it  ;
      dbs_it = dbs_.find(current_db_);
      current_db_engine =dbs_it->second;

    cout << "Database Changed. " << endl;

    return  DB_SUCCESS;

  }
  else{
    printf("No such database.\n");
  }
  return  DB_FAILED;

}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }

  printf("+----------------------|\n");
  string dbname = current_db_engine->db_file_name_;
  dbname = dbname.substr(0, dbname.size()-3);
  printf("| Tables_%s", dbname.c_str());
  for(unsigned int i = 0; i < 14-dbname.size(); i++) printf(" ");
  printf("|\n");
  printf("|----------------------|\n");
  vector<TableInfo* > tables_now;
//  unordered_map<std::string, DBStorageEngine *>::iterator dbs_it  ;
//  dbs_it = dbs_.find(current_db_);
//  DBStorageEngine *  current_db_engine =dbs_it->second;
  dberr_t show = current_db_engine->catalog_mgr_->GetTables(tables_now);
  int num = 0;
  if(show == DB_TABLE_NOT_EXIST){
    printf("| ");
    for(int i=20;i>=0; i--){
      printf(" ");
    }
    printf("|\n");
    printf("|----------------------|\n");
    cout << "\033[32m[Rows]\033[0m " << 0 << " row affected. " << endl;
    return DB_TABLE_NOT_EXIST;
  }
  for(auto table_it=tables_now.begin();table_it<tables_now.end();table_it++){
    string table_name = (*table_it)->GetTableName();
    printf("| ");
    printf("%s", table_name.c_str());
    for(int i=20-table_name.length();i>=0; i--){
      printf(" ");
    }
    printf("|\n");
    num ++;
  }
  printf("|----------------------|\n");
  cout << "\033[32m[Rows]\033[0m " << num << " rows affected. " << endl;
  return DB_SUCCESS;
//  return DB_FAILED;
}


dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }

//  DBStorageEngine * current_db_engine = dbs_[current_db_];
//  unordered_map<std::string, DBStorageEngine *>::iterator dbs_it  ;
//  dbs_it = dbs_.find(current_db_);
//  DBStorageEngine *  current_db_engine =dbs_it->second;
  pSyntaxNode table_name_node = ast->child_;
  string table_name = table_name_node->val_;

  vector<Column*>column_;
  pSyntaxNode column_attribute_now = table_name_node->next_->child_;

  while( column_attribute_now != nullptr && column_attribute_now->type_==kNodeColumnDefinition ){
    // is unique?
    bool  unique = false;
    if(column_attribute_now->val_ != nullptr){
      string is_unique = column_attribute_now->val_;
//      printf("ok\n");
      if(is_unique == "unique" ){
        unique = true;
      }
    }
    string column_name = column_attribute_now->child_->val_;
    string column_type = column_attribute_now->child_->next_->val_;
    int index=0;
    Column * now_column;
    if( column_type == "int"){
      now_column = new Column(column_name,kTypeInt, index, true, unique);
    }
    else if(column_type=="char"){
      // char length must be positive integer

      string strlength = (column_attribute_now->child_->next_->child_->val_);
      int find = strlength.find('.');
      if (find != -1){
        printf("\033[31m[ERROR]\033[0m String Length must be integer!\n");
        return DB_FAILED;
      }

      int length = atoi(column_attribute_now->child_->next_->child_->val_);
      if(length <0){
        printf("\033[31m[ERROR]\033[0m String Length must be positive!\n");
        return DB_FAILED;
      }
      now_column = new Column(column_name,kTypeChar,length, index, true, unique);
    }
    else if(column_type=="float"){

      now_column = new Column(column_name,kTypeFloat,index,true,unique);
    }
    else{
      printf("\033[31m[ERROR]\033[0m No such type: %s!\n", column_type.c_str());
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
    printf("\033[31m[ERROR]\033[0m Table already exist!\n");
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
  // pk
    current_catalog->CreateIndex(table_name,table_name+"_PK",PK,nullptr,index_info);

    }
    //unique
    vector<Column*>::iterator column_it ;
    for(column_it  = column_.begin() ; column_it < column_.end(); column_it ++){
      if((*column_it)->IsUnique()){
        //create index
        CatalogManager* current_catalog = current_db_engine->catalog_mgr_;
        IndexInfo* index_info=nullptr;
        vector <string>unique_index_name = {(*column_it)->GetName()};
        current_catalog->CreateIndex(table_name,table_name+"_"+(*column_it)->GetName()+"_unique",unique_index_name,nullptr,index_info);

      }


  }
//  return DB_TABLE_NOT_EXIST;
  cout << "\033[32m[Rows]\033[0m " << 1 << " row affected. " << endl;
  return DB_SUCCESS;
}




dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }

//  DBStorageEngine * current_db_engine ;
  dberr_t Dropped=current_db_engine->catalog_mgr_->DropTable(ast->child_->val_);
  if(Dropped==DB_TABLE_NOT_EXIST){
    printf("\033[33m[Warning]\033[0m Table not exist!\n");
    return DB_TABLE_NOT_EXIST;
  }
  cout << "\033[32m[Rows]\033[0m " << 1 << " row affected. " << endl;
  return DB_SUCCESS;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }
  printf("+----------------------+\n");
  string dbname = current_db_engine->db_file_name_;
  dbname = dbname.substr(0, dbname.size()-3);
  printf("| Indexes_%s", dbname.c_str());
  for(unsigned int i = 0; i < 13-dbname.size(); i++) printf(" ");
  printf("|\n");
  printf("+----------------------+\n");

  vector<TableInfo* > tables;
  current_db_engine->catalog_mgr_->GetTables(tables);
  vector<TableInfo* >::iterator  tables_it;
  int num = 0;
  for(tables_it=tables.begin();tables_it<tables.end();tables_it++){
    string dbname = (*tables_it)->GetTableName();
    printf("| Table %s:", dbname.c_str());
    // got index
    for(unsigned int i = 0; i < 14-dbname.size(); i++) printf(" ");
    printf("|\n");

    vector<IndexInfo*> index_now;
    current_db_engine->catalog_mgr_->GetTableIndexes((*tables_it)->GetTableName(),index_now);
    vector<IndexInfo*>::iterator index_now_it;
    for(index_now_it=index_now.begin();index_now_it<index_now.end();index_now_it++){
      num ++;
      string index_name = (*index_now_it)->GetIndexName();
      printf("|   ");

      printf("%s",index_name.c_str());

      for(int i=19-index_name.length();i > 0; i--){
        printf(" ");
      }
      printf("|\n");
    }
    printf("+----------------------+\n");
  }
  cout << "\033[32m[Rows]\033[0m " << num << " rows affected. " << endl;
  return DB_SUCCESS;

}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }
  pSyntaxNode table_name_node = ast->child_->next_;
  string table_name = table_name_node->val_;
  CatalogManager* current_catalog=current_db_engine->catalog_mgr_;
  TableInfo *table_info = nullptr;
  current_catalog->GetTable(table_name, table_info);
  pSyntaxNode key_name_node= table_name_node->next_->child_;
  bool have_unique = false;
  while (key_name_node!= nullptr){


    uint32_t key_index;
    dberr_t Getindex = table_info->GetSchema()->GetColumnIndex(key_name_node->val_,key_index);
    if (Getindex ==DB_COLUMN_NAME_NOT_EXIST){
      printf("\033[31m[ERROR]\033[0m No corresponding attribute!\n");
      return DB_FAILED;
    }
    const Column* key=table_info->GetSchema()->GetColumn(key_index);
    if(key->IsUnique()==true){
        have_unique = true;
    }

    key_name_node=key_name_node->next_;
  }
  if(have_unique == false){
      printf("\033[31m[ERROR]\033[0m Can't create index on non-unique attribute!\n");
      return DB_FAILED;
  }

  vector<string> index_keys;
  vector<string>::iterator index_keys_it;
  pSyntaxNode index_key_node= ast->child_->next_->next_->child_;
  while (index_key_node!= nullptr){
    // cout << (void*)index_key_node << endl;
    index_keys.push_back(string(index_key_node->val_) );
    index_key_node = index_key_node->next_;
  }
  IndexInfo* index_info=nullptr;
  string index_name = ast->child_->val_;
  dberr_t Created=current_catalog->CreateIndex(table_name,index_name,index_keys,nullptr,index_info);
  if(Created==DB_TABLE_NOT_EXIST){
    printf("\033[31m[ERROR]\033[0m Table not exist!\n");
  }
  if(Created==DB_INDEX_ALREADY_EXIST){

    printf("\033[31m[ERROR]\033[0m Index already exist!\n");
  }

  TableHeap* table_heap = table_info->GetTableHeap();
  vector<uint32_t>index_column_number;
  for (index_keys_it = index_keys.begin(); index_keys_it < index_keys.end() ; index_keys_it++ ){
    uint32_t index ;
    table_info->GetSchema()->GetColumnIndex(*index_keys_it,index);
    index_column_number.push_back(index);
  }
  vector<Field>fields;
//  TableIterator table_heap_it;
  for (auto table_heap_it =table_heap->Begin(nullptr) ; table_heap_it!= table_heap->End(); table_heap_it++) {
    Row &it =  *table_heap_it;
//    Row &it_row = const_cast< Row >(it);
    vector<Field> index_fields;
    for (auto index_column_number_it=index_column_number.begin();index_column_number_it<index_column_number.end();index_column_number_it++){
      index_fields.push_back(*(it.GetField(*index_column_number_it)));
    }
    Row index_row(index_fields);
    index_info->GetIndex()->InsertEntry(index_row,it.GetRowId(),nullptr);
  }

  if (Created == DB_SUCCESS)
    cout << "\033[32m[Rows]\033[0m " << 1 << " row affected. " << endl;
  return Created;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }
  string index_name_drop=ast->child_->val_;
  vector<TableInfo* > tables;
  current_db_engine->catalog_mgr_->GetTables(tables);
  vector<TableInfo* >::iterator  tables_it;
  for(tables_it=tables.begin();tables_it<tables.end();tables_it++){
    vector<IndexInfo*> index_now;
    current_db_engine->catalog_mgr_->GetTableIndexes((*tables_it)->GetTableName(),index_now);
    vector<IndexInfo*>::iterator index_now_it;
    for(index_now_it=index_now.begin();index_now_it<index_now.end();index_now_it++){
      string index_name = (*index_now_it)->GetIndexName();
      if(index_name_drop == index_name){
        dberr_t Dropped=current_db_engine->catalog_mgr_->DropIndex((*tables_it)->GetTableName(),index_name_drop);

        if(Dropped==DB_INDEX_NOT_FOUND){
          printf("\033[33m[Warning]\033[0m Index not found. \n");
        }
        cout << "\033[32m[Rows]\033[0m " << 1 << " row affected. " << endl;
        return DB_SUCCESS;
      }

    }


  }

  printf("\033[31m[ERROR]\033[0m Index drop failed. \n");
  return DB_FAILED;
}


vector<Row*> SelectTuple(pSyntaxNode sn, std::vector<Row*>& r, TableInfo* t, CatalogManager* c){
  if(sn == nullptr) return r;
  if(sn->type_ == kNodeConnector){
    // cout << "01" << endl;
    vector<Row*> ans;
    if(strcmp(sn->val_,"and") == 0){
      // cout << 1 << endl;
      auto r1 = SelectTuple(sn->child_,r,t,c);
      ans = SelectTuple(sn->child_->next_,r1,t,c);
      return ans;
    }
    else if(strcmp(sn->val_,"or") == 0){
      // cout << 2 << endl;
      auto r1 = SelectTuple(sn->child_,r,t,c);
      auto r2 = SelectTuple(sn->child_->next_,r,t,c);
      for(uint32_t i=0;i<r1.size();i++){
        ans.push_back(r1[i]);        
      }
      for(uint32_t i=0;i<r2.size();i++){
        int flag=1;//
        for(uint32_t j=0;j<r1.size();j++){
          int f=1;
          for(uint32_t k=0;k<r1[i]->GetFieldCount();k++){
            if(!r1[i]->GetField(k)->CompareEquals(*r2[j]->GetField(k))){
              f=0;break;
            }
          }
          if(f==1){
            flag=0;//
            break;}
        }
        if(flag==1) ans.push_back(r2[i]);        
      } 
      return ans;
    }
  }
  if(sn->type_ == kNodeCompareOperator){
    // cout << "02" << endl;
    string op = sn->val_;//operation type
    string col_name = sn->child_->val_;//column name
    string val = sn->child_->next_->val_;//compare value
    uint32_t keymap;
    vector<Row*> ans;
    if(t->GetSchema()->GetColumnIndex(col_name, keymap)!=DB_SUCCESS){
      cout<<"column not found. "<<endl;
      return ans;
    }
    const Column* key_col = t->GetSchema()->GetColumn(keymap);
    TypeId type =  key_col->GetType();

    if(op == "="){
      // cout << 1 << endl;
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);

        vector <IndexInfo*> indexes;
        c->GetTableIndexes(t->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
            if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
              if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                cout<<"--select using index--"<<endl;
                Row tmp_row(vect_benchmk);
                vector<RowId> result;
                (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
                for(auto q:result){
                  if(q.GetPageId()<0) continue;
                  Row *tr = new Row(q);
                  t->GetTableHeap()->GetTuple(tr,nullptr);
                  ans.push_back(tr);
                }
                return ans;
              }
            }
        }   

        
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()+2];
        strcpy(ch,val.c_str());//input compare object
        // cout<<"ch "<<sizeof(ch)<<endl;

        Field benchmk(kTypeChar,ch,key_col->GetLength(),true);
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);

        vector <IndexInfo*> indexes;
        c->GetTableIndexes(t->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
            if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
              if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                
                for(uint32_t i=0;i<r.size();i++){
                  const char* test = r[i]->GetField(keymap)->GetData();
                  // cout<<"tuple len "<<sizeof(test)<<" "<<test<<endl;
                  int eq=1;
//                  for(uint32_t q = 0;q<sizeof(test)+2;q++){
//                    if(test[q]!=ch[q]) eq=0;
//                  }
                  bool isequal = strcmp(test,ch);
                  if(isequal == 0){
                    eq=1;
                  }
                  else{
                    eq = 0;
                  }
                  // string ts = test;
                  if(eq==1){
                    vector<Field> f;
                    for(auto it:r[i]->GetFields()){
                      f.push_back(*it);
                    }
                    Row* tr = new Row(*r[i]);
                    ans.push_back(tr);
                    break;
                  }
                }

                // cout<<"--select using index--"<<endl;
                // Row tmp_row(vect_benchmk);
                // vector<RowId> result;
                // (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
                // cout<<"find num "<<result.size()<<endl;
                // for(auto q:result){
                //   if(q.GetPageId()<0) continue;
                //   cout<<"index found"<<endl;
                //   Row *tr = new Row(q);
                //   t->GetTableHeap()->GetTuple(tr,nullptr);
                //   ans.push_back(tr);
                // }
                return ans;
              }
            }
        } 

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          // cout<<"tuple len "<<sizeof(test)<<" "<<test<<endl;
          int eq=1;
          for(uint32_t q = 0;q<sizeof(test)+2;q++){
            if(test[q]!=ch[q]) eq=0;
          }
          // string ts = test;
          if(eq==1){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()+2];
        strcpy(ch,val.c_str());
        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          if(strcmp(test,ch)<0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)>0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<="){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)<=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">="){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
        
          if(strcmp(test,ch)>=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<>"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"incomparable. "<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)!=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    return ans;
  }
  return r; 
}


dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  pSyntaxNode range = ast->child_;
  vector<uint32_t> columns;
  string table_name=range->next_->val_;
  TableInfo *tableinfo = nullptr;
  dberr_t GetRet = current_db_engine->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"\033[31m[ERROR]\033[0m Table not exist. "<<endl;
    return DB_FAILED;
  }
  if(range->type_ == kNodeAllColumns){
    // cout<<"select all"<<endl;
    for(uint32_t i=0;i<tableinfo->GetSchema()->GetColumnCount();i++)
      columns.push_back(i);
  }
  else if(range->type_ == kNodeColumnList){
    // vector<Column*> all_columns = tableinfo->GetSchema()->GetColumns();
    pSyntaxNode col = range->child_;
    while(col!=nullptr){
      uint32_t pos;
      if(tableinfo->GetSchema()->GetColumnIndex(col->val_,pos)==DB_SUCCESS){
        columns.push_back(pos);
      }
      else{
        cout<<"\033[31m[ERROR]\033[0m Column not found. "<<endl;
        return DB_FAILED;
      }
      col = col->next_;
    }
  }
  vector<int> width;
  int towidth = 0;
  int temp;
  for(auto i:columns){
    temp = tableinfo->GetSchema()->GetColumn(i)->GetName().size();
    // cout << "dfs" << temp << endl;
    width.push_back( temp );
    towidth += temp + 5;
  }
  cout<<"+";
  for(uint32_t j = 0; j < width.size(); j++) {
    for(int i = 0; i < width[j]+4; i++) {
      cout << "-";
    }
    cout << "+";
  }
  cout << endl;
  for(auto i:columns){
    cout<< "|  " << tableinfo->GetSchema()->GetColumn(i)->GetName()<< "  ";
  }
  cout << "|" << endl;
  cout<<"+";
  for(uint32_t j = 0; j < width.size(); j++) {
    for(int i = 0; i < width[j]+4; i++) {
      cout << "-";
    }
    cout << "+";
  }
  cout << endl;

  if(range->next_->next_==nullptr)//
  {
    int cnt=0;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      for(uint32_t j=0;j<columns.size();j++){
        cout << "| ";
        if(it->GetField(columns[j])->IsNull()){
          cout << setw(width[j]+3) << "null";
        }
        else
          // cout << setw(width[j]+3) << it->GetField(columns[j])->GetData();
          it->GetField(columns[j])->fprint();
          cout << "  ";
      }
      cout << "|" << endl;
      cnt++;
    }
    if (cnt == 0) {
      cout << "|";
      for(int j = 0; j < towidth - 1; j++) cout << " " ;
      cout << "|" << endl;
    }
    cout<<"+";
    for(uint32_t j = 0; j < width.size(); j++) {
      for(int i = 0; i < width[j]+4; i++) {
        cout << "-";
      }
      cout << "+";
    }
    cout << endl;

    cout<<"\033[32m[Rows]\033[0m Select Success, Affects "<<cnt<<" Record. "<<endl;
    return DB_SUCCESS;
  }
  else if(range->next_->next_->type_ == kNodeConditions){
    pSyntaxNode cond = range->next_->next_->child_;
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }    
    auto ptr_rows  = SelectTuple(cond, *&origin_rows,tableinfo,current_db_engine->catalog_mgr_);
    
    for(auto it=ptr_rows.begin();it!=ptr_rows.end();it++){

      for(uint32_t j=0;j<columns.size();j++){
        cout << "| ";
        if((*it)->GetField(columns[j])->IsNull()){
          cout << setw(width[j]+3) << "null";
        }
        else
          // cout << setw(width[j]+3) << (*it)->GetField(columns[j])->GetData();
          (*it)->GetField(columns[j])->fprint();
          cout << "  ";
      }
      cout << "|" << endl;
    }
    if (ptr_rows.size() == 0) {
      cout << "|";
      for(int j = 0; j < towidth - 1; j++) cout << " " ;
      cout << "|" << endl;
    }
    cout<<"+";
    for(uint32_t j = 0; j < width.size(); j++) {
      for(int i = 0; i < width[j]+4; i++) {
        cout << "-";
      }
      cout << "+";
    }
    cout << endl;

    cout<<"\033[32m[Rows]\033[0m Select Success, Affects "<<ptr_rows.size()<<" Record. "<<endl;
  }
  
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }

  // PrintNode(ast, 0);

  string tablename(ast->child_->val_);
  TableInfo* tableinf;
  dberr_t result;
  // get the table heap to insert
  result = current_db_engine->catalog_mgr_->GetTable(tablename, tableinf);
  if (result != DB_SUCCESS) return result;

  Schema* schema = tableinf->GetSchema();
  TableHeap* tableheap = tableinf->GetTableHeap();


  // deal with the date to store
  vector<Field> fields;
  pSyntaxNode column_node = ast->child_->next_->child_;
  for(uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (column_node == NULL) {
      //has null,其他部分全是空的
      for ( uint32_t j = i ; j < schema->GetColumnCount(); j ++ ){
        Field new_field(tableinf->GetSchema()->GetColumn(j)->GetType());
        fields.push_back(new_field);
      }
      break;

//      return DB_FAILED;
    }


    TypeId tp = schema->GetColumn(i)->GetType();
    if(column_node->val_==nullptr ){
      //cout<<"has null!"<<endl;
      Field new_field(tp);
      fields.push_back(new_field);
    }
    else{
      // push back
      if (tp == kTypeInt) {
        int32_t val;
        sscanf(column_node->val_, "%d", &val);
        fields.push_back(Field(kTypeInt, val));
      }
      else if (tp == kTypeFloat) {
        float val;
        sscanf(column_node->val_, "%f", &val);
        fields.push_back(Field(kTypeFloat, val));
      }
      else if(tp == kTypeChar) {
        fields.push_back(Field(kTypeChar, column_node->val_, strlen(column_node->val_), true));
      }
      else {
        return DB_FAILED;
      }
    }

    column_node = column_node->next_;
  }
  if(column_node!=nullptr){
    // more
    printf("\033[31m[ERROR]\033[0m Too many attributes.\n ");
    return  DB_FAILED;
  }
  Row row(fields);
  bool  inserted = tableheap->InsertTuple(row, nullptr);

  if(inserted == false){
    printf("\033[31m[ERROR]\033[0m Insert failed.\n");
    return  DB_FAILED;
  }
  else{
    vector <IndexInfo*> all_indexs;
    current_db_engine->catalog_mgr_->GetTableIndexes(tablename,all_indexs);
    vector <IndexInfo*> ::iterator  index_it;
    // get all index
    for(index_it =all_indexs.begin();index_it<all_indexs.end();index_it++){

      IndexSchema* index_schema = (*index_it)->GetIndexKeySchema();
      vector<Field> index_fields;
      for(uint32_t colum_no =0; colum_no < index_schema->GetColumnCount() ;colum_no++){

        Column* column_now = const_cast<Column*>(index_schema->GetColumn(colum_no));
        index_id_t index_id;
        if(tableinf->GetSchema()->GetColumnIndex(column_now->GetName(),index_id)==DB_SUCCESS){
          index_fields.push_back(fields[index_id]);
        }
      }
      Row row_index(index_fields);
      dberr_t Inserted=(*index_it)->GetIndex()->InsertEntry(row_index,row.GetRowId(),nullptr);
      if(Inserted==DB_FAILED){

        printf("\033[31m[ERROR]\033[0m Insert failed.\n");
        vector <IndexInfo*> ::iterator  index_it_undo;
        for(index_it_undo =all_indexs.begin();index_it_undo!=index_it ;index_it_undo++){
          IndexSchema* index_schema_undo = (*index_it_undo)->GetIndexKeySchema();
          vector<Field> index_fields_undo;
          for(uint32_t colum_no_undo =0; colum_no_undo < index_schema_undo->GetColumnCount() ;colum_no_undo++){
            Column* column_undo = const_cast<Column*>(index_schema_undo->GetColumn(colum_no_undo));
            index_id_t index_id_undo;
            if(tableinf->GetSchema()->GetColumnIndex(column_undo->GetName(),index_id_undo)==DB_SUCCESS){
              index_fields_undo.push_back(fields[index_id_undo]);
            }
          }
          Row index_row_undo(index_fields_undo);
          (*index_it_undo)->GetIndex()->RemoveEntry(index_row_undo,row.GetRowId(),nullptr);
        }
        tableheap->MarkDelete(row.GetRowId(),nullptr);
        return Inserted;
      }
      else{

      }

    }

  }
   cout << "\033[32m[Rows]\033[0m " << 1 << " row affected. " << endl;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  string table_name=ast->child_->val_;
  TableInfo *tableinfo = nullptr;
  dberr_t GetRet = current_db_engine->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"\033[31m[ERROR]\033[0m Table not exist. "<<endl;
    return DB_FAILED;
  }
  TableHeap* tableheap=tableinfo->GetTableHeap();//
  auto del = ast->child_;
  vector<Row*> tar;
  if(del->next_==nullptr){//
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      tar.push_back(tp);
    }  
  }
  else{
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    
    tar  = SelectTuple(del->next_->child_, *&origin_rows,tableinfo,current_db_engine->catalog_mgr_); 
    
  }
  for(auto it:tar){
    tableheap->ApplyDelete(it->GetRowId(),nullptr);
  }
  cout<<"\033[32m[Rows]\033[0m Delete Success, Affects "<<tar.size()<<" Record!"<<endl;
  vector <IndexInfo*> indexes;//indexinfo
  current_db_engine->catalog_mgr_->GetTableIndexes(table_name,indexes);
  for(auto p=indexes.begin();p!=indexes.end();p++){
    for(auto j:tar){
      vector<Field> index_fields;
      for(auto it:(*p)->GetIndexKeySchema()->GetColumns()){
        index_id_t tmp;
        if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){
          index_fields.push_back(*j->GetField(tmp));
        }
      }
      Row index_row(index_fields);
      (*p)->GetIndex()->RemoveEntry(index_row,j->GetRowId(),nullptr);
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  string table_name(ast->child_->val_);
  TableInfo *tableinfo = nullptr;
  dberr_t resGot;
  resGot = current_db_engine->catalog_mgr_->GetTable(table_name, tableinfo);
  if (resGot == DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return resGot;
  }

  // Schema* schema = tableinfo->GetSchema();
  TableHeap* tableheap = tableinfo->GetTableHeap();//
  
  SyntaxNode* updates = ast->child_->next_;
  vector<Row*> Got;

  if(updates->next_==nullptr){
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Got.push_back(new Row(*it));
    }   
  }
  else{
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    Got  = SelectTuple(updates->next_->child_, *&origin_rows,tableinfo,current_db_engine->catalog_mgr_);
  }
  updates = updates->child_;
  while(updates && updates->type_ == kNodeUpdateValue){
    string col = updates->child_->val_;
    string upval = updates->child_->next_->val_;
    uint32_t index;//
    tableinfo->GetSchema()->GetColumnIndex(col,index);
    TypeId tid = tableinfo->GetSchema()->GetColumn(index)->GetType();
    if(tid == kTypeInt){
      Field* newval = new Field(kTypeInt,stoi(upval));
      for(auto it:Got){
        it->GetFields()[index] = newval;
      }
    }
    else if(tid == kTypeFloat){
      Field* newval = new Field(kTypeFloat,stof(upval));
      for(auto it:Got){
        it->GetFields()[index] = newval;
      }
    }
    else if(tid == kTypeChar){
      uint32_t len = tableinfo->GetSchema()->GetColumn(index)->GetLength();
      char* tc = new char[len];
      strcpy(tc,upval.c_str());
      Field* newval = new Field(kTypeChar,tc,len,true);
      for(auto it:Got){
        it->GetFields()[index] = newval;
      }
    }
    updates = updates->next_;
  }
  for(auto it:Got){
    tableheap->UpdateTuple(*it,it->GetRowId(),nullptr);
  }
  cout<<"\033[32m[Rows]\033[0m Update Success, Affects "<<Got.size()<<" Record!"<<endl;
  return DB_SUCCESS;
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
  string file_name = ast->child_->val_;
  string file_path = "testfile/"+ file_name;
  ifstream file_;
  file_.open(file_path.data());
  if(file_.is_open()){
    string sql;
    while (getline(file_,sql)){
      // create buffer for sql input
      YY_BUFFER_STATE bp = yy_scan_string(sql.c_str());
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);

      // init parser module
      MinisqlParserInit();

      // parse
      yyparse();

      ExecuteContext context_;
      Execute(MinisqlGetParserRootNode(), &context_);
//      sleep(1);
      usleep(100);

      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();

    }
    return DB_SUCCESS;
  }
  else{
    printf("\033[31m[ERROR]\033[0m Failed to open file.\n");
    return DB_FAILED;
  }



}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}

