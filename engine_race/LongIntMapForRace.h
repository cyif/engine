//
// Created by Wayne Wang on 2018/10/28.
//

#ifndef POLAR_RACE_LONGLONGMAP_H
#define POLAR_RACE_LONGLONGMAP_H

#include <malloc.h>
#include <assert.h>
#include <cmath>
#include <chrono>

class LongIntMapForRace {
public:
    explicit LongIntMapForRace() {
        this->keyMixer = initialKeyMixer();
        this->assigned = 0;
        allocateBuffers();
    };

    ~LongIntMapForRace() {
        free(keys);
        free(values);
    };

    int size() {
        return assigned;
    }

    int put(const long &key, const int &value) {
        assert(this->assigned < mask + 1);
        if(key == 0) {
            hasEmptyKey = true;
            int previousValue = values[mask + 1];
            values[mask + 1] = value;
            return previousValue;
        } else {
            int slot = hashKey(key) & mask;
            long existing;
            while ((existing = keys[slot]) != 0) {
                if(existing == key) {
                    int previousValue = values[slot];
                    values[slot] = value;
                    return previousValue;
                }
                slot = (slot + 1) & mask;
            }
            keys[slot] = key;
            values[slot] = value;
            assigned++;
            return 0;
        }

    };

    int getOrDefault(const long & key, const int &defaultValue) {
        if(key == 0) {
            return hasEmptyKey ? values[mask + 1]:defaultValue;
        } else {
            int slot = hashKey(key) & mask;
            long exsisting;
            while((exsisting = keys[slot])!=0) {
                if(exsisting == key) {
                    return values[slot];
                }
                slot = (slot + 1) & mask;
            }
            return defaultValue;
        }
    }

private:
    long *keys;
    int *values;
    int keyMixer;
    int assigned;
    int mask;
    int hasEmptyKey;

    void allocateBuffers() {
        int arraySize = 1024 * 1024;
        int emptyElementSlot = 1;
        this->keys = static_cast<long *>(malloc((arraySize + emptyElementSlot) * sizeof(long)));
        this->values = static_cast<int *>(malloc((arraySize + emptyElementSlot) * sizeof(int)));
        this->mask = arraySize - 1;

    }

    int hashKey(const long &key) {
        assert(key != 0);
        return mix(key, this->keyMixer);

    }

    int initialKeyMixer() {
        return static_cast<int>(-1686789945);
    }

    int mix(long key, int seed) {
        return static_cast<int>(mix64(key ^ seed));
    }

    long mix64(long z) {
        z = (z ^ (z >> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >> 29)) * 0xfc12c5b19d3259e9L;
        return z ^ (z >> 32);
    }


    int nextHighestPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }
};



#endif //POLAR_RACE_LONGLONGMAP_H
