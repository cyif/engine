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
    int id;
    string path;

    int fd;
    size_t filePosition;
    size_t globalOffset;

    int cacheBufferPosition;
    char *cacheBuffer;

    int keyBufferPosition;
    u_int64_t *keyBuffer;

    bool enlarge = false;
public:
    KeyValueLog(const std::string &path, const int &id, const int &fd, const size_t &globalOffset, char *cacheBuffer, u_int64_t *keyBuffer) {
        this->id = id;
        this->path = path;

        this->fd = fd;
        this->globalOffset = globalOffset;
        this->cacheBuffer = cacheBuffer;
        this->filePosition = 0;
        this->cacheBufferPosition = 0;

        this->keyBuffer = keyBuffer;
        this->keyBufferPosition = 0;

        std::ostringstream fp;
        fp << path << "/enlarge-" << id;
        string filePath = fp.str();

        if (access(filePath.data(), 0) != -1) {
            valueLogEnlargeMtx.lock();
            this->enlarge = true;

            std::ostringstream fp;
            fp << path << "/enlarge-" << id;

            this->fd = open(fp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
            fallocate(this->fd, 0, 0, VALUE_ENLARGE_SIZE + KEY_ENLARGE_SIZE);
            this->globalOffset = 0;

            this->keyBuffer = static_cast<u_int64_t *>(mmap(nullptr, KEY_ENLARGE_SIZE, PROT_READ | PROT_WRITE,
                                                            MAP_SHARED | MAP_POPULATE, this->fd,
                                                            (off_t) VALUE_ENLARGE_SIZE));
            valueLogEnlargeMtx.unlock();
        }
    }

    ~KeyValueLog() {
        if (this->cacheBufferPosition != 0) {
            auto remainSize = (size_t ) cacheBufferPosition << 12;
            pwrite(this->fd, cacheBuffer, remainSize, globalOffset + filePosition);
        }
        if (this->enlarge) {
            munmap(keyBuffer, KEY_ENLARGE_SIZE);
            close(this->fd);
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
            if (filePosition + BLOCK_SIZE > VALUE_LOG_SIZE && filePosition <= VALUE_LOG_SIZE) {
                //Enlarge Log
                valueLogEnlargeMtx.lock();
                this->enlarge = true;

                std::ostringstream fp;
                fp << path << "/enlarge-" << id;

                int fdEnlarge = open(fp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
                fallocate(fdEnlarge, 0, 0, VALUE_ENLARGE_SIZE + KEY_ENLARGE_SIZE);

                u_int64_t * keyBufferEnlarge = static_cast<u_int64_t *>(mmap(nullptr, KEY_ENLARGE_SIZE, PROT_READ | PROT_WRITE,
                                                                MAP_SHARED | MAP_POPULATE, fdEnlarge,
                                                                (off_t) VALUE_ENLARGE_SIZE));

                // 复制原来的log内容到新的文件
                for (int pos = 0; pos < filePosition; pos += BLOCK_SIZE) {
                    pread(this->fd, cacheBuffer, BLOCK_SIZE, globalOffset + pos);
                    pwrite(fdEnlarge, cacheBuffer, BLOCK_SIZE, pos);
                }

                memcpy(keyBufferEnlarge, keyBuffer, KEY_LOG_SIZE);

                this->fd = fdEnlarge;
                this->globalOffset = 0;
                this->keyBuffer = keyBufferEnlarge;

                valueLogEnlargeMtx.unlock();
            }
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
