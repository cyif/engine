//
// Created by parallels on 11/14/18.
//

#ifndef ENGINE_SORT_LOG_H
#define ENGINE_SORT_LOG_H

#include <malloc.h>
#include <iostream>


class SortLog {

private:
    u_int64_t *keys;
    int *values;
    int nums;
    int arraySize;

public:

    explicit SortLog(int capacity) :
            arraySize(capacity),
            nums(0) {
        this->keys = static_cast<u_int64_t *>(malloc((arraySize) * sizeof(u_int64_t)));
        this->values = static_cast<int *>(malloc((arraySize) * sizeof(int)));
    };

    ~SortLog() {
        free(keys);
        free(values);
    };

    int size() {
        return nums;
    }

    void put(const u_int64_t &key, const int &value) {
        keys[nums] = key;
        values[nums] = value;
        nums++;
    };

    void quicksort() {
        if (nums > 0)
            quicksort(0, nums - 1);
    }

    void quicksort(int pLeft, int pRight) {
        if (pLeft < pRight) {
            int storeIndex = partition(pLeft, pRight);
            quicksort(pLeft, storeIndex - 1);
            quicksort(storeIndex + 1, pRight);
        }
    }

    int partition(int pLeft, int pRight) {

        swap(pRight, (pLeft + pRight) / 2);

        u_int64_t pivotKey = keys[pRight];
        int pivotValue = values[pRight];

        int storeIndex = pLeft;
        for (int i = pLeft; i < pRight; i++) {
            if (keys[i] < pivotKey || (keys[i] == pivotKey && values[i] < pivotValue)) {
                swap(i, storeIndex);
                storeIndex++;
            }

        }
        swap(storeIndex, pRight);
        return storeIndex;
    }

     void swap(int left, int right) {
        u_int64_t tmp_key = keys[left];
        keys[left] = keys[right];
        keys[right] = tmp_key;

        int tmp_value = values[left];
        values[left] = values[right];
        values[right] = tmp_value;
    }



     int find(const u_int64_t & key) {
        int left = 0;
        int right = nums - 1;
        int middle;
        while (left <= right) {
            middle = left + (right - left) / 2;
            if (keys[middle] == key) {
                while (middle + 1 <= nums - 1 && keys[middle + 1] == key)
                    middle++;
                return values[middle];
            } else if (key < keys[middle]) {
                right = middle - 1;
            } else {
                left = middle + 1;
            }
        }
        return -1;
    }

};


#endif //ENGINE_SORT_LOG_H
