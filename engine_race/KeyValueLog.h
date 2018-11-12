//
// Created by Wayne Wang on 2018/11/8.
//

#ifndef ENGINE_RACE_KEYVALUELOG_H
#define ENGINE_RACE_KEYVALUELOG_H

#include <stdint.h>
#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sparsepp/spp.h>
#include <include/engine.h>
#include <unistd.h>
#include <mutex>
#include "include/polar_string.h"
#include "LongIntMapForRace.h"

using spp::sparse_hash_map;


namespace polar_race {


    class KeyValueLog {
    public:
        size_t KEY_BUFFER_SIZE = 12 * 1024 * 1024;

        KeyValueLog(const std::string &path, const long &id) {
            this->id = id;

            //获得Key file path
            std::ostringstream ss1;
            ss1 << path << "/key-" << this->id;
            this->keyFilePath = ss1.str();

            //获得value file path
            std::ostringstream ss2;
            ss2 << path << "/value-" << this->id;
            this->valueFilePath = ss2.str();

            //设置keymap的load factor和大小
//            this->keyMap.resize(KEY_BUFFER_SIZE);
//            this->keyMap.min_load_factor(1.0);
//            this->keyMap.max_load_factor(1.0);

            //打开key文件并且映射mmap
            this->keyFd = open(this->keyFilePath.data(), O_CREAT | O_RDWR, 0777);
            fallocate(this->keyFd, 0, 0, KEY_BUFFER_SIZE);
            this->keyBuffer = static_cast<u_int8_t *>(mmap(nullptr, KEY_BUFFER_SIZE, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_POPULATE,
                                                           this->keyFd, 0));

            int max = 0;
            //读取key
            for (int i = 0; i < KEY_BUFFER_SIZE; i += 12) {
                long key;
                int value;
                key = *(long *) (keyBuffer + i);
                value = *(int *) (keyBuffer + i + 8);
                if (key == 0 && value == 0) {
                    this->keyBufferPosition = i;
                    break;
                }
                if (value > max) {
                    max = value;
                }
                keyMap.put(key, value);
            }

            //打开Value文件
            auto mode = O_CREAT | O_RDWR | O_DIRECT;
            if(this->keyMap.size() < 110000) {
                mode = O_CREAT | O_RDWR ;
            }
            this->valueFd = open(this->valueFilePath.data(), mode, 0777);
            fallocate(this->valueFd, 0, 0, 1024L * 1024 * 4096);
            this->valueFilePosition = ((long) max) * 4096;

            this->buffer = static_cast<char *>(malloc(4096));
            posix_memalign((void **) &buffer, 4096, 4096);
        }

        ~KeyValueLog() {
            munmap(keyBuffer, KEY_BUFFER_SIZE);
            close(this->keyFd);
            close(this->valueFd);
        }

        bool contains(const PolarString &key)  {
            long k = *(long *) key.data();
            return this->keyMap.getOrDefault(k, -1) != -1;

        }

        void putValue(const PolarString &key, const char *value) {
            _mutex.lock();

            memcpy(this->buffer, value, 4096);
            pwrite(this->valueFd, this->buffer, 4096, valueFilePosition);
            auto k = (long *) key.data();
            memcpy(keyBuffer + keyBufferPosition, k, 8);
            int index;
            index = static_cast<int>(valueFilePosition / 4096);
            memcpy(keyBuffer + keyBufferPosition + 8, &index, 4);
//            fprintf(stderr, "w %li, %d, %li \n", *k, index, id);

            keyBufferPosition += 12;
            valueFilePosition += 4096;
            _mutex.unlock();
        }

        RetCode readValue(const PolarString &key, char * value) {
            long k = *(long *) key.data();
            int index = keyMap.getOrDefault(k, -1);
            if (index == -1) {
//                fprintf(stderr, "not found %li, %d, %li \n", k, index, id);
                return kNotFound;
            } else {
                auto filePosition = ((long) index) * 4096;
                pread(this->valueFd, value, 4096, filePosition);
//                fprintf(stderr, "r %li, %d, %li \n", k, index, id);
                return kSucc;
            }
        }

    private:
        long id;
        std::mutex _mutex;
        std::string keyFilePath;
        std::string valueFilePath;
        int keyFd;
        int valueFd;
        off_t valueFilePosition;
        long keyBufferPosition;
        LongIntMapForRace keyMap = LongIntMapForRace();
        u_int8_t *keyBuffer;

        char * buffer;
    };
}

#endif //ENGINE_RACE_KEYVALUELOG_H
