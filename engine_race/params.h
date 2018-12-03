//
// Created by 杨煜溟 on 2018/11/29.
//

#ifndef ENGINE_PARAMS_H
#define ENGINE_PARAMS_H

#include <string>
#include <mutex>

using namespace std;

const int MAX_RANGE_COUNT = 2;

const int LOG_NUM = 4096;
const int NUM_PER_SLOT = 1024 * 16;
const size_t VALUE_LOG_SIZE = NUM_PER_SLOT * 4096;  //128mb
const size_t KEY_LOG_SIZE = NUM_PER_SLOT * 8;

const int FILE_NUM = 64;

const int SORT_LOG_SIZE = NUM_PER_SLOT;
const int SORT_FILE_SIZE = SORT_LOG_SIZE * (LOG_NUM / FILE_NUM);

const int KEY_ENLARGE_SIZE = 20010 * 8;
const int VALUE_ENLARGE_SIZE = 20010 * 4096;
const int SORT_ENLARGE_SIZE = 20010;

const size_t CACHE_SIZE = VALUE_LOG_SIZE;
const int CACHE_NUM = 4;

const int PAGE_PER_BLOCK = 4;
const size_t BLOCK_SIZE = PAGE_PER_BLOCK * 4096;

const int RECOVER_THREAD = 64;
const int READDISK_THREAD = 2;

const int MAX_LENGTH_INSERT_SORT = 12;

std::mutex sortLogEnlargeMtx;
std::mutex valueLogEnlargeMtx;

#endif //ENGINE_PARAMS_H
