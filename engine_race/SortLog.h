//
// Created by parallels on 11/14/18.
//

#ifndef ENGINE_SORT_LOG_H
#define ENGINE_SORT_LOG_H

#include <malloc.h>
#include <iostream>
#include "params.h"


namespace polar_race {
    class SortLog {

    private:
        u_int64_t keys[NUM_PER_SLOT];
        u_int32_t values[NUM_PER_SLOT];
        int nums = 0;

    public:

        int size() {
            return nums;
        }

        bool hasGreaterEqualKey(u_int64_t key) {
            return __builtin_bswap64(key) <= keys[nums - 1];
        }

        bool hasLessKey(u_int64_t key) {
            return __builtin_bswap64(key) > keys[0];
        }

        void put(u_int64_t &bigEndkey, const u_int32_t &value) {
            keys[nums] = __builtin_bswap64(bigEndkey);
//        keys[nums] = bigEndkey;
            values[nums] = value;
            nums++;
        };

        void quicksort() {
            if (nums > 0) {
                quicksort(0, nums - 1);
                int k = 0;
                for (int i = 0; i < nums; i++)
                    if (i == nums - 1 || keys[i] != keys[i + 1]) {
                        keys[k] = keys[i];
                        values[k] = values[i];
                        k++;
                    }
                nums = k;
            }
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
            if (left == right) return;
            keys[left] ^= keys[right] ^= keys[left] ^= keys[right];
            values[left] ^= values[right] ^= values[left] ^= values[right];
        }

        long find(u_int64_t &bigEndkey) {

            u_int64_t key = __builtin_bswap64(bigEndkey);
//        u_int64_t key = bigEndkey;

            int left = 0;
            int right = nums - 1;
            int middle;
            while (left <= right) {
                middle = (left + right) / 2;
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

        int findValueByIndex(int index) {
            return values[index];
        }

        u_int64_t findKeyByIndex(int index) {
            return __builtin_bswap64(keys[index]);
        }

        int getMinIndexGreaterEqualThan(u_int64_t key) {
            key = __builtin_bswap64(key);

            int left = 0;
            int right = nums - 1;
            int middle;
            while (left < right) {
                middle = (left + right) / 2;
                if (key > keys[middle])
                    left = middle + 1;
                else
                    right = middle;
            }
            return right;
        }


        int getMaxIndexLessThan(u_int64_t key) {
            key = __builtin_bswap64(key);

            int left = 0;
            int right = nums - 1;
            while (left <= right) {
                int middle = (left + right) / 2;
                if (key > keys[middle])
                    left = middle + 1;
                else
                    right = middle - 1;
            }
            return right;
        }

    };
}


#endif //ENGINE_SORT_LOG_H