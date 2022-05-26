#include "record/row.h"

/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *  
 *
 */

static u_char bytes[8] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};

// set the bitmap to 1
static void setBitMap(u_char* bitmap, uint32_t position) {
  bitmap[position/(8*sizeof(char))] |= bytes[position%(8*sizeof(char))];
}

// get the bitmap
static bool getBitMap(u_char* bitmap, uint32_t position) {
  if(bitmap[position/(8*sizeof(char))] & bytes[position%(8*sizeof(char))])
    return true;
  else
    return false;
}

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t size = fields_.size();
  MACH_WRITE_TO(uint32_t, buf, size); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);
  char * bitmapadd = buf;

  // set the bitmap
  uint32_t bitmapsize = (size-1)/(8*sizeof(char)) + 1;
  if(size == 0) bitmapsize = 1;
  u_char * bitmap = new u_char[bitmapsize];
  for(u_int32_t i = 0; i < bitmapsize; i++) bitmap[i] = 0;
  buf += sizeof(char)*bitmapsize;

  // serialize the field
  for(u_int32_t i = 0; i < size; i++) {
    buf += fields_[i]->SerializeTo(buf);
    if (fields_[i]->IsNull())
      setBitMap(bitmap, i);
  }
  memcpy(bitmapadd, bitmap, bitmapsize);
  delete[] bitmap;
  return GetSerializedSize(schema);
}

//#include <iostream>
//using namespace std;
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t size = MACH_READ_FROM(uint32_t, buf); 
  buf += Type::GetTypeSize(TypeId::kTypeInt);

  fields_.clear();
  
  // read the bitmap
  uint32_t bitmapsize = (size-1)/(8*sizeof(char)) + 1;
  if(size == 0) bitmapsize = 1;
  u_char * bitmap = new u_char[bitmapsize];
  memcpy(bitmap, buf, bitmapsize);
  buf += bitmapsize*sizeof(char);

  bool isnull;
  Field * field = NULL;
  for(u_int32_t i = 0; i < size; i++) {
    isnull = getBitMap(bitmap, i);
    // cout << schema->GetColumn(i)->GetType() << " " << isnull << endl;
    // cout << schema->GetColumn(i)->GetType() << TypeId::kTypeInt << " " << isnull << endl;
    // cout << heap_ << endl;
    buf += Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &field, isnull, heap_);
    fields_.push_back(field);
  }
  delete[] bitmap;
  return GetSerializedSize(schema);
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t size = fields_.size();
  uint32_t buf = Type::GetTypeSize(TypeId::kTypeInt);

  // set the bitmap
  if(size != 0)
    buf += (size-1)/(8*sizeof(char)) + 1;
  else 
    buf += 1;

  // serialize the field
  for(u_int32_t i = 0; i < size; i++) {
    buf += fields_[i]->GetSerializedSize();
  }
  return buf;
}
