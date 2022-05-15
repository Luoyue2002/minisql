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
  for(int i = 0; i < name_.length(); i++) {
    MACH_WRITE_TO(char, buf, name_[i]);
    buf += Type::GetTypeSize(TypeId::kTypeChar);
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
  return Type::GetTypeSize(TypeId::kTypeInt)*4 + Type::GetTypeSize(TypeId::kTypeChar)*( name_.length()+1);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  return 0;
}
