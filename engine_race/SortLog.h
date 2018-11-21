//
// Created by parallels on 11/14/18.
//

#ifndef ENGINE_SORT_LOG_H
#define ENGINE_SORT_LOG_H

#include <malloc.h>
#include <iostream>

namespace polar_race {
    class SortLog {

    private:
        u_int64_t *keys;
        u_int32_t *values;
        int nums;
        int arraySize;

    public:

        explicit SortLog(int capacity) :
                arraySize(capacity),
                nums(0) {
            this->keys = static_cast<u_int64_t *>(malloc((arraySize) * sizeof(u_int64_t)));
            this->values = static_cast<u_int32_t *>(malloc((arraySize) * sizeof(u_int32_t)));
        };

        ~SortLog() {
            free(keys);
            free(values);
        };

        int size() {
            return nums;
        }

        bool hasGreaterEqualKey(u_int64_t key) {
            return swapEndian(key) <= keys[nums - 1];
        }

        bool hasLessKey(u_int64_t key) {
            return swapEndian(key) > keys[0];
        }

        u_int64_t swapEndian(u_int64_t key) {
            return (((key & 0x00000000000000FF) << 56) |
                    ((key & 0x000000000000FF00) << 40) |
                    ((key & 0x0000000000FF0000) << 24) |
                    ((key & 0x00000000FF000000) << 8) |
                    ((key & 0x000000FF00000000) >> 8) |
                    ((key & 0x0000FF0000000000) >> 24) |
                    ((key & 0x00FF000000000000) >> 40) |
                    ((key & 0xFF00000000000000) >> 56));
        }

        void put(u_int64_t &bigEndkey, const u_int32_t &value) {
            keys[nums] = swapEndian(bigEndkey);
//        keys[nums] = bigEndkey;
            values[nums] = value;
            nums++;
        };

        void quicksort() {
            if (nums > 0) {
                quicksort(0, nums - 1);
                int k = 0;
                for (int i = 0; i < nums; i++){
                    if (i == nums - 1 || keys[i] != keys[i + 1]) {
                        keys[k] = keys[i];
                        values[k] = values[i];
                        k++;
                    }
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

        int find(u_int64_t &bigEndkey) {

            u_int64_t key = swapEndian(bigEndkey);
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

        u_int32_t findValueByIndex(int index) {
            return values[index];
        }

        u_int64_t findKeyByIndex(int index) {
            return swapEndian(keys[index]);
        }

        int getMinIndexGreaterEqualThan(u_int64_t key) {
            key = swapEndian(key);

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
            key = swapEndian(key);

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