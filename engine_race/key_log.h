//
// Created by parallels on 11/11/18.
//

#ifndef ENGINE_KEY_LOG_H
#define ENGINE_KEY_LOG_H

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


using namespace std;

namespace polar_race {


    class KeyLog {
    public:

        KeyLog(const std::string &path, const int &id, const size_t &size) : id(id), keyBufferPosition(0) {
            //获得Key file path
            std::ostringstream ss;
            ss << path << "/key-" << this->id;
            this->filePath = ss.str();
            //打开key文件并且映射mmap
            this->fd = open(this->filePath.data(), O_CREAT | O_RDWR, 0777);
            this->keyBufferSize = size;
            fallocate(this->fd, 0, 0, this->keyBufferSize);
            this->keyBuffer = static_cast<char *>(mmap(nullptr, this->keyBufferSize, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_POPULATE, this->fd, 0));
            this->keyBufferPosition = 0;
        }

        ~KeyLog() {
            munmap(keyBuffer, this->keyBufferSize);
            close(this->fd);
        }


        inline void putValue(const char * key) {
            *((u_int64_t *) (keyBuffer + keyBufferPosition)) = *((u_int64_t *) key);
            keyBufferPosition += 8;
        }

        inline bool getKey(u_int64_t & key) {
            key = *(u_int64_t*)(keyBuffer + keyBufferPosition);
            keyBufferPosition += 8;
            return key != 0;
        }

        void setKeyBufferPosition(long position) {
            this->keyBufferPosition = position;
        }

    private:
        int id;
        int fd;
        std::string filePath;
        long keyBufferPosition;
        char * keyBuffer;
        size_t keyBufferSize;
    };
}
#endif //ENGINE_KEY_LOG_H
