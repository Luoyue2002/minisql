#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    uint32_t *offset;
    offset = &page_offset;
    uint32_t byte_index = *offset / 8;
    uint8_t bit_index = *offset % 8;
    if ( IsPageFree(*offset) ){
        bytes[byte_index] = bytes[byte_index] | bytebit[bit_index & 7];
        page_allocated_ = page_allocated_ + 1;
        uint32_t i;
        for( i=0; i< GetMaxSupportedSize() ; i++){
            if(IsPageFree(i)){
                next_free_page_ = i;
                break;
            }
        }
        return true;
    }
    else
        return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    if ( !IsPageFree(page_offset) ){
        bytes[byte_index] = bytes[byte_index] | bytebit[bit_index & 7];
        page_allocated_ = page_allocated_ - 1;
        if ( page_offset < next_free_page_)
            next_free_page_ = page_offset;
        return true;
    }
    else
        return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    bool isfree = IsPageFreeLow( byte_index , bit_index);
    return isfree;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    if(bytes[byte_index] & bytebit[bit_index & 7]) // this bit is 1
        return false;
    else
        return true;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;