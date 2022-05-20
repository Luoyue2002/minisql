#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

#include "buffer/buffer_pool_manager.h"
#include "page/table_page.h"
#include "storage/table_iterator.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(TablePage* page, RowId rowid, Schema* schema, BufferPoolManager* buffer_pool_manager );

  // explicit TableIterator(const TableIterator &other);
  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator& operator++(int);

private:
  // add your own private member variables here
  TablePage* page_;
  RowId row_id_;
  Schema* schema_;
  Row* row;
  BufferPoolManager* buffer_pool_manager_;
};

#endif //MINISQL_TABLE_ITERATOR_H
