//
// Created by parallels on 11/11/18.
//

#ifndef ENGINE_VALUE_LOG_H
#define ENGINE_VALUE_LOG_H

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

namespace polar_race {


    class ValueLog {

    private:
        int fd;
        off_t filePosition;
        off_t globalOffset;


        int cacheBufferPosition;
        u_int8_t *cacheBuffer{};


    public:

        ValueLog(const int &fd, const off_t &globalOffset, u_int8_t *cacheBuffer) : fd(fd),
                                                                                    globalOffset(globalOffset),
                                                                                    cacheBuffer(cacheBuffer),
                                                                                    filePosition(0),
                                                                                    cacheBufferPosition(0) {
        }

        off_t size() {
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

        inline void readValue(off_t offset, char *value, size_t size) {
            pread(this->fd, value, size, globalOffset + offset);
        }

        void recover(u_int32_t sum) {
            this->filePosition = (off_t) sum << 12;
            this->cacheBufferPosition = sum % PAGE_PER_BLOCK;
            auto offset = (size_t) cacheBufferPosition << 12;
            filePosition -= offset;
            pwrite(this->fd, cacheBuffer, offset, globalOffset + filePosition);
        }


    };
}
#endif //ENGINE_VALUE_LOG_H