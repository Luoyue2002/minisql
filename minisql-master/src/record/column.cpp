#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }

}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}


/*
  We store the column in the format: 
  | COLUMN_MAGIC_NUM int | name char* |
  | len int | table_ind int | typeid + bool*2 char |
*/

uint32_t Column::SerializeTo(char *buf) const {
  MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  MACH_WRITE_TO(uint32_t, buf, name_.length());
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  // write name
  for(u_int32_t i = 0; i < name_.length(); i++) {
    MACH_WRITE_TO(char, buf, name_[i]);
    buf += sizeof(char);
  }
  MACH_WRITE_TO(uint32_t, buf, len_);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  MACH_WRITE_TO(uint32_t, buf, table_ind_);
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  // edit the char
  u_char c = 0;
  if(type_ == TypeId::kTypeInt) c = 1;
  else if(type_ == TypeId::kTypeFloat) c = 2;
  else if(type_ == TypeId::kTypeChar) c = 3;
  c = (c << 2)&0xFC;
  if(nullable_) c = c|0x2;
  if(unique_) c = c|0x1;
  MACH_WRITE_TO(char, buf, c);

  return GetSerializedSize();
}

uint32_t Column::GetSerializedSize() const {
  return Type::GetTypeSize(TypeId::kTypeInt)*4 + sizeof(char)*( name_.length()+1);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  if (column != nullptr) {
    return 0;
  }

  //Deserialize
  std::string column_name = "";
  TypeId type;
  uint32_t len;
  uint32_t col_ind;
  bool nullable = false;
  bool unique = false;
  uint32_t MAGIC_NUM;
  char c;

  MAGIC_NUM = MACH_READ_FROM(u_int32_t, buf);
  if(MAGIC_NUM != 210928) return 0;
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  len = MACH_READ_FROM(u_int32_t, buf); // length of the name
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  // get the name
  for(u_int32_t i = 0; i < len; i++) {
    c = MACH_READ_FROM(char, buf);
    buf += sizeof(char);
    column_name.push_back(c);
  }
  len = MACH_READ_FROM(u_int32_t, buf);
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  col_ind = MACH_READ_FROM(u_int32_t, buf); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  // the index
  c = MACH_READ_FROM(char, buf); 
  buf += sizeof(char);
  if(c & 0x2) nullable = true;
  if(c & 0x1) unique = true;
  // get the type
  if(((c >> 2) & 0x3F) == 1) type = TypeId::kTypeInt;
  else if(((c >> 2) & 0x3F) == 2) type = TypeId::kTypeFloat;
  else if(((c >> 2) & 0x3F) == 3) type = TypeId::kTypeChar;
  else type = TypeId::kTypeInvalid;

  if (type != TypeId::kTypeChar)
    column = ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  else 
    column = ALLOC_P(heap, Column)(column_name, type, len, col_ind, nullable, unique);
  return column->GetSerializedSize();
}
