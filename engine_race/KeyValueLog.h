//
// Created by 杨煜溟 on 2018/12/2.
//

#ifndef ENGINE_KEYVALUELOG_H
#define ENGINE_KEYVALUELOG_H

#include <stdint.h>
#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include "../include/polar_string.h"
#include "../include/engine.h"
#include "params.h"

class KeyValueLog {
private:
    int fd;
    size_t filePosition;
    size_t globalOffset;

    int cacheBufferPosition;
    char *cacheBuffer;

    int keyBufferPosition;
    u_int64_t *keyBuffer;
public:
    KeyValueLog(const int &fd, const size_t &globalOffset, char *cacheBuffer, u_int64_t *keyBuffer) {
        this->fd = fd;
        this->globalOffset = globalOffset;
        this->cacheBuffer = cacheBuffer;
        this->filePosition = 0;
        this->cacheBufferPosition = 0;

        this->keyBuffer = keyBuffer;
        this->keyBufferPosition = 0;
    }

    ~KeyValueLog() {
        if (this->cacheBufferPosition != 0) {
            auto remainSize = (size_t ) cacheBufferPosition << 12;
            pwrite(this->fd, cacheBuffer, remainSize, globalOffset + filePosition);
        }
    }

    size_t size() {
        return filePosition + (cacheBufferPosition << 12);
    }

    inline void putValue(const char *value) {
        memcpy(cacheBuffer + (cacheBufferPosition << 12), value, 4096);
        cacheBufferPosition++;
        if (cacheBufferPosition == PAGE_PER_BLOCK) {
            pwrite(this->fd, cacheBuffer, BLOCK_SIZE, globalOffset + filePosition);
            filePosition += BLOCK_SIZE;
            cacheBufferPosition = 0;
        }
    }

    inline void readValue(long index, char *value) {
        pread(this->fd, value, 4096, globalOffset + (index << 12));
    }

    inline void readValue(size_t offset, char *value, size_t size) {
        pread(this->fd, value, size, globalOffset + offset);
    }

    void recover(u_int32_t sum) {
        this->filePosition = (size_t) sum << 12;
    }

    inline void putKey(const char * key) {
        *(keyBuffer + keyBufferPosition) = *((u_int64_t *) key);
        keyBufferPosition++;
    }

    inline bool getKey(u_int64_t & key) {
        key = *(keyBuffer + keyBufferPosition);
        keyBufferPosition++;
        return key != 0;
    }

    void setKeyBufferPosition(int position) {
        this->keyBufferPosition = position;
    }
};


#endif //ENGINE_KEYVALUELOG_H
