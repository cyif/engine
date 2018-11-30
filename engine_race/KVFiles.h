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
        int valueFd{};

        int cacheFd;
        char *cacheBuffer;
        size_t cacheFileSize;

        int keyFd{};
        u_int64_t * keyBuffer;
        size_t keyFileSize;

    public:
        KVFiles(const std::string &path, const int &id, const long &valueFileSize, const size_t &cacheFileSize, const size_t &keyFileSize) {
            //Value Log
            std::ostringstream fp;
            fp << path << "/value-" << id;
            this->valueFd = open(fp.str().data(), O_CREAT | O_RDWR | O_DIRECT, 0777);
            fallocate(this->valueFd, 0, 0, valueFileSize);


            //Value Cache
            std::ostringstream cfp;
            cfp << path << "/value-cache-" << id;
            this->cacheFileSize = cacheFileSize;
            //value cache file
            this->cacheFd = open(cfp.str().data(), O_CREAT | O_RDWR, 0777);
            fallocate(this->cacheFd, 0, 0, cacheFileSize);
            this->cacheBuffer = static_cast<char *>(mmap(nullptr, cacheFileSize, PROT_READ | PROT_WRITE,
                                                             MAP_SHARED | MAP_POPULATE, this->cacheFd, 0));


            //Key Log
            std::ostringstream ss;
            ss << path << "/key-" << id;
            this->keyFd = open(ss.str().data(), O_CREAT | O_RDWR, 0777);
            this->keyFileSize = keyFileSize;
            fallocate(this->keyFd, 0, 0, this->keyFileSize);
            this->keyBuffer = static_cast<u_int64_t *>(mmap(nullptr, this->keyFileSize, PROT_READ | PROT_WRITE,
                                                           MAP_SHARED | MAP_POPULATE, this->keyFd, 0));
        }

        ~KVFiles() {
            close(this->valueFd);
            munmap(cacheBuffer, cacheFileSize);
            close(this->cacheFd);
            munmap(keyBuffer, this->keyFileSize);
            close(this->keyFd);
        }

        char *getCacheBuffer() {
            return cacheBuffer;
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
