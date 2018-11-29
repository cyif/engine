//
// Created by 杨煜溟 on 2018/11/29.
//

#ifndef ENGINE_PARAMS_H
#define ENGINE_PARAMS_H

#include <string>

const int MAX_RANGE_COUNT = 2;

const int LOG_NUM = 2048;
const int NUM_PER_SLOT = 1024 * 32;
const size_t VALUE_LOG_SIZE = NUM_PER_SLOT * 4096;  //128mb
const size_t KEY_LOG_SIZE = NUM_PER_SLOT * 8;

const size_t FILE_NUM = 256;

const size_t CACHE_SIZE = VALUE_LOG_SIZE;
const int CACHE_NUM = 4;
const int CACHE_BLOCK_SIZE = VALUE_LOG_SIZE;   //64mb

const size_t PAGE_PER_BLOCK = 4;
const size_t BLOCK_SIZE = PAGE_PER_BLOCK * 4096;

const int RECOVER_THREAD = 64;
const int READDISK_THREAD = 2;

#endif //ENGINE_PARAMS_H
