//
// Created by 杨煜溟 on 2018/11/28.
//

#ifndef ENGINE_VALUELOGFILE_H
#define ENGINE_VALUELOGFILE_H


#include <stdint.h>
#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include "../include/engine.h"

namespace polar_race {


    class KVFiles {
    private:
        int valueFd;
        size_t valueFileSize;

        u_int64_t * keyBuffer;
        size_t keyFileSize;

    public:
        KVFiles(const std::string &path, const int &id, const size_t &valueFileSize, const size_t &keyFileSize) : valueFileSize(valueFileSize), keyFileSize(keyFileSize){
            //Value Log
            std::ostringstream fp;
            fp << path << "/value-" << id;
            this->valueFd = open(fp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
            fallocate(this->valueFd, 0, 0, valueFileSize + keyFileSize);

            this->keyBuffer = static_cast<u_int64_t *>(mmap(nullptr, keyFileSize, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_POPULATE, this->valueFd,
                                                            (off_t) valueFileSize));
        }

        ~KVFiles() {
            close(this->valueFd);
        }


        int getValueFd() const {
            return valueFd;
        }

        u_int64_t *getKeyBuffer() const {
            return keyBuffer;
        }
    };
}


#endif //ENGINE_VALUELOGFILE_H
