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
#include "LongIntMapForRace.h"
#include "../include/polar_string.h"
#include "../include/engine.h"
using namespace std;

namespace polar_race {


    class KeyLog {
    public:
        size_t KEY_BUFFER_SIZE = 12 * 1024 * 1024;
//        size_t KEY_BUFFER_SIZE = 12 * 1024 * 8;


        KeyLog(const std::string &path, const int &id) : id(id), keyBufferPosition(0) {
            //获得Key file path
            std::ostringstream ss;
            ss << path << "/key-" << this->id;
            this->filePath = ss.str();
            //打开key文件并且映射mmap
            this->fd = open(this->filePath.data(), O_CREAT | O_RDWR, 0777);
            fallocate(this->fd, 0, 0, KEY_BUFFER_SIZE);
            this->keyBuffer = static_cast<u_int8_t *>(mmap(nullptr, KEY_BUFFER_SIZE, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_POPULATE, this->fd, 0));
        }

        ~KeyLog() {
            munmap(keyBuffer, KEY_BUFFER_SIZE);
            close(this->fd);
        }


        void putValue(const PolarString &key, const int value) {
            auto k = (long *) key.data();
            memcpy(keyBuffer + keyBufferPosition, k, 8);
            memcpy(keyBuffer + keyBufferPosition + 8, &value, 4);
            keyBufferPosition += 12;
        }


        bool getKey(long & key, int & value, int pos) {
            key = *(long *)  (keyBuffer + pos);
            value = *(int *) (keyBuffer + pos + 8);
            return (key != 0 || value != 0);
        }

        void setKeyBufferPosition(long position) {
            this->keyBufferPosition = position;
        }

    private:
        int id;
        int fd;
        std::string filePath;
        long keyBufferPosition;
        u_int8_t *keyBuffer;
    };
}
#endif //ENGINE_KEY_LOG_H
