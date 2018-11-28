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


    class ValueLogFile {
    private:
        int fd{};
        std::string filePath;

    public:
        ValueLogFile(const std::string &path, const int &id, const long &size) {
            //获得value file path
            std::ostringstream fp;
            fp << path << "/value-" << id;
            this->filePath = fp.str();

            //打开Value文件
            auto mode = O_CREAT | O_RDWR | O_DIRECT;
            this->fd = open(this->filePath.data(), mode, 0777);
            fallocate(this->fd, 0, 0, size);
        }

        ~ValueLogFile() {
            close(this->fd);
        }

        int getFd() {
            return fd;
        }
    };
}


#endif //ENGINE_VALUELOGFILE_H
