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

    cout << "welcome to our minisql" <<endl;
    // 进入目录读文件
    vector<string> file_name  = GetFiles(temp.c_str(), ext.c_str());
    // ------------------------test
    vector<string>::iterator file_name_it  ;

    // change the cd dir there, to make new DBStorageEngine Work in the path
    chdir("database");

    if(!file_name.empty()) {
      for (file_name_it = file_name.begin(); file_name_it != file_name.end(); file_name_it++) {
        printf("%s\n",file_name_it->c_str());
        string db_name = *file_name_it + ".db";
//        string db_name = *file_name_it ;
        DBStorageEngine *storage_engine_ = new DBStorageEngine(db_name.c_str(), false, DEFAULT_BUFFER_POOL_SIZE);

        dbs_.emplace(*file_name_it, storage_engine_);
      }
    }


    for (file_name_it = file_name.begin(); file_name_it != file_name.end(); file_name_it++) {
      cout << *file_name_it << " test " << endl;
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

      unordered_map<std::string, DBStorageEngine *>::iterator dbs_it  ;
      dbs_it = dbs_.find(current_db_);
      current_db_engine =dbs_it->second;


    return  DB_SUCCESS;

  }
  else{
    printf("no such database\n");
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

  printf("|----------------------|\n");
  printf("|  show tables         |\n");
  printf("|----------------------|\n");
  vector<TableInfo* > tables_now;
//  unordered_map<std::string, DBStorageEngine *>::iterator dbs_it  ;
//  dbs_it = dbs_.find(current_db_);
//  DBStorageEngine *  current_db_engine =dbs_it->second;
  dberr_t show = current_db_engine->catalog_mgr_->GetTables(tables_now);
  if(show == DB_TABLE_NOT_EXIST){
    printf("| ");
    for(int i=20;i>=0; i--){
      printf(" ");
    }
    printf("|\n");
    printf("|----------------------|\n");
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
  }
  printf("|----------------------|\n");
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

  while( column_attribute_now->next_ != nullptr && column_attribute_now->type_==kNodeColumnDefinition ){
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
        printf("String Length must be integer!\n");
        return DB_FAILED;
      }

      int length = atoi(column_attribute_now->child_->next_->child_->val_);
      if(length <0){
        printf("String Length must be positive!\n");
        return DB_FAILED;
      }
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
    printf("db table not exist!\n");
    return DB_TABLE_NOT_EXIST;
  }
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
  printf("|----------------------|\n");
  printf("|  show indexes        |\n");
  printf("|----------------------|\n");
  vector<TableInfo* > tables;
  current_db_engine->catalog_mgr_->GetTables(tables);
  vector<TableInfo* >::iterator  tables_it;
  for(tables_it=tables.begin();tables_it<tables.end();tables_it++){
    printf("index of Table: %s\n", (*tables_it)->GetTableName().c_str());
    // got index
    vector<IndexInfo*> index_now;
    current_db_engine->catalog_mgr_->GetTableIndexes((*tables_it)->GetTableName(),index_now);
    vector<IndexInfo*>::iterator index_now_it;
    for(index_now_it=index_now.begin();index_now_it<index_now.end();index_now_it++){
      string index_name = (*index_now_it)->GetIndexName();
      for(int i=5;i>=0; i--){
        printf(" ");
      }

      printf("%s\n",index_name.c_str());

      for(int i=15-index_name.length();i>=0; i--){
        printf(" ");
      }
    }
    printf("|----------------------|\n");
  }
  return DB_SUCCESS;

  return DB_FAILED;
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
  while (key_name_node!= nullptr){


    uint32_t key_index;
    dberr_t Getindex = table_info->GetSchema()->GetColumnIndex(key_name_node->val_,key_index);
    if (Getindex ==DB_COLUMN_NAME_NOT_EXIST){
      printf("Attribute isn't in The Table!\n");
      return DB_FAILED;
    }
    const Column* key=table_info->GetSchema()->GetColumn(key_index);
    if(key->IsUnique()==false){
      printf("Can't Create Index On Non-unique Key!\n");
      return DB_FAILED;
    }

    key_name_node=key_name_node->next_;
  }

  vector<string> index_keys;
  vector<string>::iterator index_keys_it;
  pSyntaxNode index_key_node= ast->child_->next_->next_->child_;
  while (index_key_node!= nullptr){

    index_keys.push_back(key_name_node->val_);
    index_key_node = index_key_node->next_;
  }
  IndexInfo* index_info=nullptr;
  string index_name = ast->child_->val_;
  dberr_t Created=current_catalog->CreateIndex(table_name,index_name,index_keys,nullptr,index_info);
  if(Created==DB_TABLE_NOT_EXIST){
    printf("table not exist!\n");
  }
  if(Created==DB_INDEX_ALREADY_EXIST){

    printf("index already exist!\n");
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
    const Row &it =  *table_heap_it;
//    Row &it_row = const_cast< Row >(it);
    vector<Field> index_fields;
    for (auto index_column_number_it=index_column_number.begin();index_column_number_it<index_column_number.end();index_column_number_it++){
      index_fields.push_back(*(it.GetField(*index_column_number_it)));
    }
    Row index_row(index_fields);
    index_info->GetIndex()->InsertEntry(index_row,it.GetRowId(),nullptr);
  }


  return Created;
      return DB_FAILED;
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
          printf("Index Not Found\n");
        }
        return DB_SUCCESS;
      }

    }


  }


  printf("Index drop failed\n");
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }

  return DB_FAILED;
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
    printf("too many attributes");
    return  DB_FAILED;
  }
  Row row(fields);
  bool  inserted = tableheap->InsertTuple(row, nullptr);

  //所有索引维护
  if(inserted == false){
    printf("insert failed");
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
        // 撤销操作

        printf("insert failed");
        //把前面插入过的全都撤销掉
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

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }

  string table_name=ast->child_->val_;
  TableInfo *table_info = nullptr;
  dberr_t GetTable = current_db_engine->catalog_mgr_->GetTable(table_name, table_info);
  if (GetTable==DB_TABLE_NOT_EXIST){
    printf("Table Not Exist!");
    return DB_FAILED;
  }

  TableHeap* table_heap=table_info->GetTableHeap();
  SyntaxNode* delete_node = ast->child_;
  vector<Row*> delete_row;

  if(delete_node == nullptr){
    for(TableIterator it = table_info->GetTableHeap()->Begin(nullptr);it != table_info->GetTableHeap()->End();it++){
      Row* temp = new Row(*it);
      delete_row.push_back(temp);
    }

  }
  else{
    vector<Row*> origin_rows;
    for(TableIterator it = table_info->GetTableHeap()->Begin(nullptr);it != table_info->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    ///--------------------------------------------------------------------------------------------------------
//    delete_row  = rec_sel(delete_node->next_->child_, *&origin_rows,table_info,current_db_engine->catalog_mgr_);
   ///---------------------------------------------------------------------------------------------------------

  }

  for(auto it:delete_row){
    table_heap->ApplyDelete(it->GetRowId(),nullptr);
  }

  printf("delete success , %zu records delete!\n", delete_row.size());

  /// index 维护
  vector <IndexInfo*> index_now;
  current_db_engine->catalog_mgr_->GetTableIndexes(table_name,index_now);

  vector <IndexInfo*>::iterator  index_now_it;
  for(index_now_it=index_now.begin();index_now_it!=index_now.end();index_now_it++){
    for(auto row_now:delete_row){
      vector<Field> index_fields;

      for(auto it:(*index_now_it)->GetIndexKeySchema()->GetColumns()){
        index_id_t index_id;
        if(table_info->GetSchema()->GetColumnIndex(it->GetName(),index_id)==DB_SUCCESS){
          Field * field_now = row_now->GetField(index_id);
          index_fields.push_back(*field_now);
        }
      }
      Row index_row(index_fields);
      (*index_now_it)->GetIndex()->RemoveEntry(index_row,row_now->GetRowId(),nullptr);
    }
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif

  if(current_db_engine == NULL) {
    printf("No database used.\n");
    return DB_FAILED;
  }

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

      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();

    }
    return DB_SUCCESS;
  }
  else{
    printf("failed to open file\n");
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
