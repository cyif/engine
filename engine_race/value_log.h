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
    public:
        size_t PAGE_PER_BLOCK = 8;
        size_t BLOCK_SIZE = PAGE_PER_BLOCK * 4096;
        bool limit = false;

        ValueLog(const std::string &path, const int &id, const long &size, u_int8_t* cacheBufferLimit) : id(id), filePosition(0) {

            //获得value file path
            std::ostringstream fp, cfp;
            fp << path << "/value-" << this->id;
            this->filePath = fp.str();
            cfp << path << "/value-cache-" << this->id;
            this->cacheFilePath = cfp.str();

            //打开Value文件
            auto mode = O_CREAT | O_RDWR | O_DIRECT;
            this->fd = open(this->filePath.data(), mode, 0777);
            fallocate(this->fd, 0, 0, size);

            //value cache file
            this->cacheFd = open(this->cacheFilePath.data(), O_CREAT | O_RDWR, 0777);
            fallocate(this->cacheFd, 0, 0, BLOCK_SIZE);
            this->cacheBuffer = static_cast<u_int8_t *>(mmap(nullptr, BLOCK_SIZE, PROT_READ | PROT_WRITE,
                                                             MAP_SHARED | MAP_POPULATE, this->cacheFd, 0));

            this->cacheBufferLimit = cacheBufferLimit;
            this->cacheBufferPosition = 0;
        }

        ~ValueLog() {

            if (!limit)
                munmap(cacheBuffer, BLOCK_SIZE);
            else {
                if (this->cacheBufferPosition != 0) {
                    auto remainSize = (size_t) cacheBufferPosition << 12;
                    pwrite(this->fd, cacheBuffer, remainSize, filePosition);
                    filePosition += remainSize;
                }
            }
            close(this->fd);
            close(this->cacheFd);
        }

        off_t size() {
            return filePosition + (cacheBufferPosition << 12);
        }

        inline void putValue(const char *value) {
            memcpy(cacheBuffer + (cacheBufferPosition << 12), value, 4096);
            cacheBufferPosition++;
            if (cacheBufferPosition == PAGE_PER_BLOCK) {
                pwrite(this->fd, cacheBuffer, BLOCK_SIZE, filePosition);
                filePosition += BLOCK_SIZE;
                cacheBufferPosition = 0;

                if (filePosition == (10000 * 4096)) {
                    limit = true;
                    munmap(cacheBuffer, BLOCK_SIZE);
                    this->cacheBuffer = cacheBufferLimit;
                }
            }
        }

        inline void readValue(int index, char *value) {
            pread(this->fd, value, 4096, ((long) index) << 12);
        }

        inline void readValue(off_t offset, char *value, size_t size) {
            pread(this->fd, value, size, offset);
        }

        void recover(u_int32_t sum) {
            this->filePosition = (off_t) sum << 12;
            this->cacheBufferPosition = sum % PAGE_PER_BLOCK;
            if (sum <= 15000) {
                auto offset = (size_t) cacheBufferPosition << 12;
                filePosition -= offset;
                pwrite(this->fd, cacheBuffer, offset, filePosition);
            }
        }

    private:
        int id;
        int fd;
        int cacheFd;
        std::string filePath;
        std::string cacheFilePath;
        off_t filePosition;
        u_int8_t *cacheBuffer;
        u_int8_t *cacheBufferLimit;
        int cacheBufferPosition;

    };
}
#endif //ENGINE_VALUE_LOG_H