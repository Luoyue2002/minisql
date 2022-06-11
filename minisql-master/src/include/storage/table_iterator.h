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
  explicit TableIterator();

  // you may define your own constructor based on your member variables
  explicit TableIterator(TableHeap *table_heap, RowId rid);

  // explicit TableIterator(const TableIterator &other);
  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator& operator++(int);

private:
  // add your own private member variables here
  TableHeap *table_heap_;
  Row *row_;
};

#endif //MINISQL_TABLE_ITERATOR_H
