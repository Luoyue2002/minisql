#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"
#include <iostream>
using namespace std;

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, DISABLED_SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);

  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 30;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  printf("keys[i],values[i]\n");
  for (int i = 0; i < n; i++) {
    printf("%d %d\n" , keys[i],values[i]);
  }

  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
//    printf("%d\n" , delete_seq[i]);
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    printf("%d\n" , i);
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  printf("check prepare\n");
  // Print tree
//  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    printf("Getvalue %d \n" , i);
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  printf("check prepare\n");
  // Delete half keys
  printf("delete_seq[i]\n");
  for (int i = 0; i < n/2 ; i++) {
    printf(" %d\n",delete_seq[i]);

    tree.Remove(delete_seq[i]);
    tree.PrintTree(mgr[i]);
  }

//   Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {

    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
//    printf("%d, %d\n",i, delete_seq[i]);
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}

TEST(BPlusTreeTests_insert, DISABLED_SampleTest) {
    // Init engine
    DBStorageEngine engine(db_name);
    cout<< "start"<<endl;
    BasicComparator<int> comparator;
    BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
    TreeFileManagers mgr("tree_");
// Prepare data
    const int n = 30;
    vector<int> keys;
    vector<int> values;
    vector<int> delete_seq;
    map<int, int> kv_map;
    for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
    }
// Shuffle data
    ShuffleArray(keys);
    ShuffleArray(values);
    ShuffleArray(delete_seq);
// Map key value
    cout<< "map"<<endl;
    for (int i = 0; i < n; i++) {

    kv_map[keys[i]] = values[i];
//    printf("%d,%d\n",keys[i], values[i]);
    }
// Insert data
    cout<< "insert"<<endl;

    printf("debug\n");
    for (int i = 0; i < n; i++) {
//      printf("start insert ");
//    printf("%d,%d,%d\n",i,keys[i], values[i]);
    tree.Insert(keys[i], values[i]);
//    tree.PrintTree(mgr[i]);
//    cout<< "insert finish"<<endl;
    }
//    cout<< "finish"<<endl;
    ASSERT_TRUE(tree.Check());
// Print tree
    tree.PrintTree(mgr[0]);
    vector<int> ans;
    for (int i = 0; i < n; i++) {
      tree.GetValue(i, ans);
//      printf("%d, %d\n",i, kv_map[i]);
//      printf("%d, %d\n",i, ans[i]);
      ASSERT_EQ(kv_map[i], ans[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Delete half keys
    for (int i = 0; i < n / 2; i++) {
      printf("%d, %d\n",i, delete_seq[i]);
      tree.Remove(delete_seq[i]);
      tree.PrintTree(mgr[i+1]);
    }
    tree.PrintTree(mgr[1]);
    // Check valid
    ans.clear();
    for (int i = 0; i < n / 2; i++) {
      ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
    }
    for (int i = n / 2; i < n; i++) {
      ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
      ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
    }
}


TEST(BPlusTreeTests_insert_Getvalue, DISABLED_SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  cout<< "start"<<endl;
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 30;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  cout<< "map"<<endl;
  keys[0]=0; values[0]=16;
  keys[1]=13; values[1]=24;
  keys[2]=22; values[2]=19;
  keys[3]=15; values[3]=20;
  keys[4]=23; values[4]=11;
  keys[5]=26; values[5]=2;
  keys[6]=2; values[6]=4;
  keys[7]=10; values[7]=18;
  keys[8]=11; values[8]=13;
  keys[9]=18; values[9]=6;
  keys[10]=14; values[10]=17;
  keys[11]=8; values[11]=27;
  keys[12]=5; values[12]=0;
  keys[13]=28; values[13]=23;
  keys[14]=7; values[14]=12;
  keys[15]=19; values[15]=15;
  keys[16]=4; values[16]=29;
  keys[17]=21; values[17]=26;
  keys[18]=27; values[18]=1;
  keys[19]=1; values[19]=8;
  keys[20]=16; values[20]=14;
  keys[21]=6; values[21]=28;
  keys[22]=12; values[22]=7;
  keys[23]=9; values[23]=22;
  keys[24]=17; values[24]=5;
  keys[25]=25; values[25]=25;
  keys[26]=3; values[26]=10;
  keys[27]=20; values[27]=9;
  keys[28]=24; values[28]=21;
  keys[29]=29; values[29]=3;






  for (int i = 0; i < n; i++) {

    kv_map[keys[i]] = values[i];
    //    printf("%d,%d\n",keys[i], values[i]);
  }
  // Insert data
  cout<< "insert"<<endl;

  printf("debug\n");
  for (int i = 0; i < n; i++) {

    tree.Insert(keys[i], values[i]);
    tree.PrintTree(mgr[i]);
  }
  cout<< "finish"<<endl;
  ASSERT_TRUE(tree.Check());
  // Print tree
//  tree.PrintTree(mgr[0]);
  vector<int> ans;
  printf("Getvalue after insert start\n");
  for (int i = 0; i < n; i++) {
    printf(" %d\n",i);
    tree.GetValue(i, ans);
//    printf("%d, %d\n",i, kv_map[i]);
//    printf("%d, %d\n",i, ans[i]);
          ASSERT_EQ(kv_map[i], ans[i]);
  }
  delete_seq[0]=18;
  delete_seq[1]=2;
  delete_seq[2]=7;
  delete_seq[3]=22;
  delete_seq[4]=26;
  delete_seq[5]=19;
  delete_seq[6]=9;
  delete_seq[7]=16;
  delete_seq[8]=3;
  delete_seq[9]=17;
  delete_seq[10]=5;
  delete_seq[11]=23;
  delete_seq[12]=25;
  delete_seq[13]=20;
  delete_seq[14]=13;
  delete_seq[15]=0;
  delete_seq[16]=8;
  delete_seq[17]=28;
  delete_seq[18]=29;
  delete_seq[19]=14;
  delete_seq[20]=24;
  delete_seq[21]=12;
  delete_seq[22]=21;
  delete_seq[23]=1;
  delete_seq[24]=6;
  delete_seq[25]=4;
  delete_seq[26]=27;
  delete_seq[27]=15;
  delete_seq[28]=11;
  delete_seq[29]=10;





  ASSERT_TRUE(tree.Check());
//  delete_seq[0]=20;
  for (int i = 0; i < n ; i++) {
    printf("%d, %d\n",i, delete_seq[i]);
    tree.Remove(delete_seq[i]);
    tree.PrintTree(mgr[i]);
  }
//  tree.PrintTree(mgr[1]);
  // Check valid
  ans.clear();
  printf("these still in tree\n");
  for (int i = n / 2; i < n/2; i++) {

    printf("%d, %d\n",i, delete_seq[i]);
  }
  for (int i = 0; i < n / 2; i++) {
    printf("%d\n",delete_seq[i]);
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}

TEST(BPlusTreeTests_Final, DISABLED_SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);

  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 30;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  printf("input");
  for (int i = 0; i < n; i++) {
      cin >> keys[i] >> values[i] ;
      printf("%d %d" , keys[i],values[i]);
  }

  for (int i = 0; i < n; i++) {
//    scanf("%d %d" &keys[i],values[i]);
    kv_map[keys[i]] = values[i];
    //    printf("%d\n" , delete_seq[i]);
  }
  // Insert data
  for (int i = 0; i < n; i++) {

    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  printf("delete_seq[i]");
  for (int i = 0; i < n / 2; i++) {
    cin >> delete_seq[i] ;
    printf(" %d\n",delete_seq[i]);
    tree.Remove(delete_seq[i]);
  }
  tree.PrintTree(mgr[1]);
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
        printf("%d, %d\n",i, delete_seq[i]);
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
        printf("%d, %d\n",i, delete_seq[i]);
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}



TEST(BPlusTreeTests_truly, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 30;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
  }
  tree.PrintTree(mgr[1]);
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}
