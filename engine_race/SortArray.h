//
// Created by 杨煜溟 on 2018/12/2.
//

#ifndef ENGINE_SORTARRAY_H
#define ENGINE_SORTARRAY_H

#include <iostream>
#include "params.h"

class SortArray {
private:
    u_int64_t keys[SORT_LOG_SIZE * LOG_NUM];
    u_int16_t values[SORT_LOG_SIZE * LOG_NUM];

public:

    u_int64_t * getKeyArray(int &i) {
        return &keys[SORT_LOG_SIZE * i];
    }

    u_int16_t * getValueArray(int &i) {
        return &values[SORT_LOG_SIZE * i];
    }

};


#endif //ENGINE_SORTARRAY_H
