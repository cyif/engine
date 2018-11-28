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


namespace polar_race {


    class ValueLog {

    private:
        int fd;
        off_t filePosition;
        off_t globalOffset;

        std::string cacheFilePath;
        int cacheFd;
        u_int8_t *cacheBuffer;
        int cacheBufferPosition;

    public:
        size_t PAGE_PER_BLOCK = 8;
        size_t BLOCK_SIZE = PAGE_PER_BLOCK * 4096;

        ValueLog(const std::string &path, const int &id, const int &fd, const off_t &globalOffset) : fd(fd),
                                                                                      globalOffset(globalOffset),
                                                                                      filePosition(0),
                                                                                      cacheBufferPosition(0) {

            //获得value file path
            std::ostringstream cfp;
            cfp << path << "/value-cache-" << id;
            this->cacheFilePath = cfp.str();

            //value cache file
            this->cacheFd = open(this->cacheFilePath.data(), O_CREAT | O_RDWR, 0777);
            fallocate(this->cacheFd, 0, 0, BLOCK_SIZE);
            this->cacheBuffer = static_cast<u_int8_t *>(mmap(nullptr, BLOCK_SIZE, PROT_READ | PROT_WRITE,
                                                             MAP_SHARED | MAP_POPULATE, this->cacheFd, 0));
        }

        ~ValueLog() {
            munmap(cacheBuffer, BLOCK_SIZE);
            close(this->cacheFd);
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