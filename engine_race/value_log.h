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
        size_t BLOCK_SIZE = 4 * 4096;
        size_t CACHE_BUFFER_SIZE = BLOCK_SIZE + 4;

        ValueLog(const std::string &path, const int &id, const long &size): id(id), filePosition(0) {

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
            fallocate(this->cacheFd, 0, 0, CACHE_BUFFER_SIZE);
            this->cacheBuffer = static_cast<u_int8_t *>(mmap(nullptr, CACHE_BUFFER_SIZE, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_POPULATE, this->cacheFd, 0));
            this->cacheBufferPosition = *(int *) (cacheBuffer + BLOCK_SIZE);

//            this->buffer = static_cast<char *>(malloc(CACHE_BUFFER_SIZE));
            posix_memalign((void **) &cacheBuffer, CACHE_BUFFER_SIZE, CACHE_BUFFER_SIZE);
        }

        ~ValueLog() {
            munmap(cacheBuffer, CACHE_BUFFER_SIZE);
            close(this->fd);
            close(this->cacheFd);
//            free(buffer);
        }


        off_t putValue(const char *value) {
//            memcpy(buffer, value, 4096);
            off_t currentPos = filePosition + (cacheBufferPosition << 12);
            memcpy(cacheBuffer + (cacheBufferPosition << 12), value, 4096);
            cacheBufferPosition ++;
            if (cacheBufferPosition == 4) {
                pwrite(this->fd, cacheBuffer, BLOCK_SIZE, filePosition);
                filePosition += BLOCK_SIZE;
                cacheBufferPosition = 0;
                memcpy(cacheBuffer + BLOCK_SIZE, &cacheBufferPosition, 4);
            } else {
                memcpy(cacheBuffer + BLOCK_SIZE, &cacheBufferPosition, 4);
            }
            return currentPos;
        }

        void readValue(int index, char *value) {
            auto filePosition = ((long) index) << 12;
            pread(this->fd, value, 4096, filePosition);
        }

        void setValueFilePosition(long position) {
            this->filePosition = position;
        }

        void flush() {
            if (cacheBufferPosition != 0) {
                pwrite(this->fd, cacheBuffer, ((size_t)cacheBufferPosition << 12), filePosition - (cacheBufferPosition << 12));
                cacheBufferPosition = 0;
                memcpy(cacheBuffer + BLOCK_SIZE, &cacheBufferPosition, 4);
            }
        }

    private:
        int id;
        int fd;
        int cacheFd;
        std::string filePath;
        std::string cacheFilePath;
        off_t filePosition;
//        char * buffer;
        u_int8_t *cacheBuffer;
        int cacheBufferPosition;

    };
}
#endif //ENGINE_VALUE_LOG_H
