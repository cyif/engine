#ifndef ENGINE_RACE_PENGINE_H
#define ENGINE_RACE_PENGINE_H


#include <iostream>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <aio.h>
#include <sys/time.h>      //添加头文件
#include <unistd.h>
#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include <map>
#include <set>
#include <condition_variable>
#include "../include/polar_string.h"
#include "../include/engine.h"
#include "key_log.h"
#include "value_log.h"
#include "SortLog.h"
#include "ThreadPool.h"
#include "ValueLogFile.h"

const int LOG_NUM = 4096;
const int NUM_PER_SLOT = 1024 * 20;

const size_t VALUE_LOG_SIZE = NUM_PER_SLOT * 4096;  //80mb
const size_t KEY_LOG_SIZE = NUM_PER_SLOT * 8;

const size_t VALUE_FILE_NUM = 256;

const size_t CACHE_SIZE = VALUE_LOG_SIZE;
const int CACHE_NUM = 16;
const int CACHE_BLOCK_SIZE = 16 * 1024 * 1024;   //16mb

const int RECOVER_THREAD = 64;
const int READDISK_THREAD = 64;


using namespace std;
using namespace std::chrono;

namespace polar_race {
    static char *prepare() {
        auto buffer = static_cast<char *>(malloc(4096));
        posix_memalign((void **) &buffer, (size_t) getpagesize(), 4096);
        return buffer;
    }

    milliseconds now() {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    }

    static thread_local std::unique_ptr<char> readBuffer(prepare());

    class PEngine {

    private:
        KeyLog **keyLogs;
        ValueLog **valueLogs;
        SortLog **sortLogs;
        ValueLogFile **valueLogFiles;
        std::mutex logMutex[LOG_NUM];

        char *valueCache;
        ThreadPool *readDiskThreadPool;
        std::atomic_flag readDiskFlag = ATOMIC_FLAG_INIT;

        std::condition_variable_any rangeCacheFinish[CACHE_NUM];
        std::mutex rangeCacheFinishMtx[CACHE_NUM];
        int rangeCacheCount[CACHE_NUM] = {0};

        std::condition_variable_any readDiskFinish[CACHE_NUM];
        std::mutex readDiskFinishMtx[CACHE_NUM];
        int readDiskCount[CACHE_NUM] = {0};

        bool isCacheReadable[CACHE_NUM];
        bool isCacheWritable[CACHE_NUM];
        int currentCacheLogId[CACHE_NUM];

        std::condition_variable_any rangeAllFinish;
        bool rangeAllFinishFlag = false;
        std::mutex rangeAllFinishMtx;

        int rangeAllFinishCount = 0;

        milliseconds start;

    public:
        explicit PEngine(const string &path) {
            this->start = now();
            // init

            this->keyLogs = static_cast<KeyLog **>(malloc(LOG_NUM * sizeof(KeyLog *)));
            this->valueLogs = static_cast<ValueLog **>(malloc(LOG_NUM * sizeof(ValueLog *)));
            this->valueLogFiles = static_cast<ValueLogFile **>(malloc(VALUE_FILE_NUM * sizeof(ValueLogFile *)));


            std::ostringstream ss;
            ss << path << "/key-0";
            string filePath = ss.str();

            int num_log_per_file = LOG_NUM / VALUE_FILE_NUM;

            if (access(filePath.data(), 0) != -1) {

                for (int fileId = 0; fileId < VALUE_FILE_NUM; fileId++) {
                    *(valueLogFiles + fileId) = new ValueLogFile(path, fileId, VALUE_LOG_SIZE * num_log_per_file);
                }

                this->sortLogs = static_cast<SortLog **>(malloc(LOG_NUM * sizeof(SortLog *)));

                for (int logId = 0; logId < LOG_NUM; logId++) {
                    *(sortLogs + logId) = new SortLog(NUM_PER_SLOT);
                    *(keyLogs + logId) = new KeyLog(path, logId, KEY_LOG_SIZE);

                    int fileId = logId % VALUE_FILE_NUM;
                    int fd = valueLogFiles[fileId]->getFd();
                    off_t globalOffset = (logId / VALUE_FILE_NUM) * VALUE_LOG_SIZE;
                    *(valueLogs + logId) = new ValueLog(path, logId, fd, globalOffset);
                }

                recover();

                this->valueCache = static_cast<char *>(malloc(CACHE_SIZE * CACHE_NUM));
                posix_memalign((void **) &valueCache, (size_t) getpagesize(), CACHE_SIZE * CACHE_NUM);

                for (int i = 0; i < CACHE_NUM; i++) {
                    isCacheReadable[i] = false;
                    isCacheWritable[i] = true;
                    currentCacheLogId[i] = -1;
                }
                readDiskThreadPool = new ThreadPool(READDISK_THREAD);

            } else {
                std::thread t[RECOVER_THREAD];

                for (int i = 0; i < RECOVER_THREAD; i++) {

                    t[i] = std::thread([i, num_log_per_file, path, this] {

                        for (int fileId = i; fileId < VALUE_FILE_NUM; fileId += RECOVER_THREAD) {
                            *(valueLogFiles + fileId) = new ValueLogFile(path, fileId,
                                                                         VALUE_LOG_SIZE * num_log_per_file);
                        }

                        for (int logId = i; logId < LOG_NUM; logId+=RECOVER_THREAD) {
                            *(keyLogs + logId) = new KeyLog(path, logId, KEY_LOG_SIZE);

                            int fileId = logId % VALUE_FILE_NUM;
                            int fd = valueLogFiles[fileId]->getFd();
                            off_t globalOffset = (logId / VALUE_FILE_NUM) * VALUE_LOG_SIZE;
                            *(valueLogs + logId) = new ValueLog(path, logId, fd, globalOffset);
                        }
                    });
                }

                for (auto &i : t) {
                    i.join();
                }
            }

            printf("Open database complete. time spent is %lims\n", (now() - start).count());
//            printf("============================Engine Start!========================\n");
        }

        ~PEngine() {
            printf("deleting engine, total life is %lims\n", (now() - start).count());
            if (sortLogs != nullptr) {
                for (int i = 0; i < LOG_NUM; i++)
                    delete sortLogs[i];
                delete[] sortLogs;
                free(valueCache);
            }

            for (int fileId = 0; fileId < VALUE_FILE_NUM; fileId++) {
                delete valueLogFiles[fileId];
            }

            for (int logId = 0; logId < LOG_NUM; logId++) {
                delete keyLogs[logId];
                delete valueLogs[logId];
            }

            delete[] keyLogs;
            delete[] valueLogs;
            delete[] valueLogFiles;

            printf("Finish deleting engine, total life is %lims\n", (now() - start).count());
        }

        void recover() {
            // recover
            std::thread t[RECOVER_THREAD];
            for (int i = 0; i < RECOVER_THREAD; i++) {
                t[i] = std::thread(&PEngine::recoverAndSort, this, i);
            }

            for (auto &i : t) {
                i.join();
            }
        }

        void recoverAndSort(const int &thread_id) {

            char log_per_thread = LOG_NUM / RECOVER_THREAD;
            int id = thread_id * log_per_thread;

            for (int i = 0; i < log_per_thread; i++) {

                KeyLog *keyLog = keyLogs[id];
                ValueLog *valueLog = valueLogs[id];
                SortLog *sortLog = sortLogs[id];

                u_int64_t k;
                u_int32_t cnt = 0;

                while (keyLog->getKey(k))
                    sortLog->put(k, cnt++);

                sortLog->quicksort();
                keyLog->setKeyBufferPosition(cnt * 8);
                valueLog->recover(cnt);

                id++;
            }
        }

        inline int getLogId(const char *k) {
            return ((u_int16_t) ((u_int8_t) k[0]) << 4) | ((u_int8_t) k[1] >> 4);
//            return (*((u_int8_t *) k));
        }

        void put(const PolarString &key, const PolarString &value) {

            auto logId = getLogId(key.data());
            logMutex[logId].lock();
//            lock_guard<mutex> lck(logMutex[logId]);
            valueLogs[logId]->putValue(value.data());
            keyLogs[logId]->putValue(key.data());
            logMutex[logId].unlock();
        }

        RetCode read(const PolarString &key, string *value) {
            auto logId = getLogId(key.data());
            auto index = sortLogs[logId]->find(*(u_int64_t *) key.data());

            if (index == -1) {
                return kNotFound;
            } else {
                auto buffer = readBuffer.get();
                valueLogs[logId]->readValue(index, buffer);
                value->assign(buffer, 4096);
                return kSucc;
            }
        }

        RetCode range(const PolarString &lower, const PolarString &upper, Visitor &visitor) {
            auto lowerKey = *((u_int64_t *) lower.data());
            auto lowerLogId = getLogId(lower.data());

            auto upperKey = *((u_int64_t *) upper.data());
            auto upperLogId = getLogId(upper.data());
            bool lowerFlag = false;
            bool upperFlag = false;

            if (lower == "") {
                lowerFlag = true;
                lowerLogId = 0;
            }

            if (upper == "") {
                upperFlag = true;
                upperLogId = LOG_NUM - 1;
            }

//            if (lower == "" && upper == "") {
//                rangeAll(visitor);
//                return kSucc;
//            }

            if (lower == "" && upper == "" && (sortLogs[0]->size() > 25000 / 2)) {
                rangeAll(visitor);
                return kSucc;
            }

            if (lowerLogId > upperLogId && !upperFlag) {
                return kInvalidArgument;
            } else {
                auto buffer = readBuffer.get();

                for (int logId = lowerLogId; logId <= upperLogId; logId++) {
                    SortLog *sortLog = sortLogs[logId];
                    ValueLog *valueLog = valueLogs[logId];
                    if ((!lowerFlag && !sortLog->hasGreaterEqualKey(lowerKey))
                        || (!upperFlag && !sortLog->hasLessKey(upperKey)))
                        break;

                    auto l = 0;
                    if (!lowerFlag && logId == lowerLogId) {
                        l = sortLog->getMinIndexGreaterEqualThan(lowerKey);
                    }
                    auto r = sortLog->size() - 1;
                    if (!upperFlag && logId == upperLogId) {
                        r = sortLog->getMaxIndexLessThan(upperKey);
                    }
                    range(l, r, sortLog, valueLog, visitor, buffer);
                }
            }
            return kSucc;
        }

        void
        range(int lowerIndex, int upperIndex, SortLog *sortLog, ValueLog *valueLog, Visitor &visitor, char *buffer) {
            for (int i = lowerIndex; i <= upperIndex; i++) {
                auto offset = sortLog->findValueByIndex(i);
                if (offset >= 0) {
                    valueLog->readValue(offset, buffer);
                    u_int64_t k = sortLog->findKeyByIndex(i);
                    visitor.Visit(PolarString(((char *) (&k)), 8), PolarString(buffer, 4096));
                }
            }
        }

        void readDisk() {
//            printf("Start Read Disk Thread %ld!\n", gettidv1());
            for (int logId = 0; logId < LOG_NUM; logId++) {

                auto cacheIndex = logId % CACHE_NUM;

                if (!isCacheWritable[cacheIndex]) {
                    //等待获取可用的cache
                    rangeCacheFinishMtx[cacheIndex].lock();
                    while (!isCacheWritable[cacheIndex]) {
                        rangeCacheFinish[cacheIndex].wait(rangeCacheFinishMtx[cacheIndex]);
                    }
                    rangeCacheFinishMtx[cacheIndex].unlock();
                }
                isCacheWritable[cacheIndex] = false;
                readDiskCount[cacheIndex] = 0;
                currentCacheLogId[cacheIndex] = logId;

                auto fileSize = valueLogs[logId]->size();
                int maxIndex = (int) (fileSize / CACHE_BLOCK_SIZE);
                auto valueLog = valueLogs[logId];
                auto cache = valueCache + cacheIndex * CACHE_SIZE;

                //这里换成线程池可能好一些
                for (int index = 0; index <= maxIndex; index++) {

                    readDiskThreadPool->enqueue([index, cacheIndex, valueLog, cache, maxIndex, fileSize, this] {
                        size_t offset = (size_t) index * CACHE_BLOCK_SIZE;
                        if (index == maxIndex) {
                            valueLog->readValue(offset, (cache + offset), (size_t) fileSize % CACHE_BLOCK_SIZE);
                        } else {
                            valueLog->readValue(offset, (cache + offset), CACHE_BLOCK_SIZE);
                        }

                        readDiskFinishMtx[cacheIndex].lock();
                        auto count = ++readDiskCount[cacheIndex];
                        if (count == maxIndex + 1) {
                            isCacheReadable[cacheIndex] = true;
                            readDiskFinish[cacheIndex].notify_all();
                        }
                        readDiskFinishMtx[cacheIndex].unlock();
                    });
                }
            }
        }


        void rangeAll(Visitor &visitor) {
            if (!readDiskFlag.test_and_set()) {
                std::thread t = std::thread(&PEngine::readDisk, this);
                t.detach();
            }

            for (int logId = 0; logId < LOG_NUM; logId++) {

                // 等待读磁盘线程读完当前valueLog
                auto cacheIndex = logId % CACHE_NUM;
                if (!isCacheReadable[cacheIndex] || currentCacheLogId[cacheIndex] != logId) {
                    readDiskFinishMtx[cacheIndex].lock();
                    while (!isCacheReadable[cacheIndex] || currentCacheLogId[cacheIndex] != logId) {
                        readDiskFinish[cacheIndex].wait(readDiskFinishMtx[cacheIndex]);
                    }
                    readDiskFinishMtx[cacheIndex].unlock();
                }

                auto cache = valueCache + cacheIndex * CACHE_SIZE;

                for (int i = 0, total = sortLogs[logId]->size(); i < total; i++) {
                    auto k = sortLogs[logId]->findKeyByIndex(i);
                    auto offset = 4096L * sortLogs[logId]->findValueByIndex(i);
                    visitor.Visit(PolarString(((char *) (&k)), 8), PolarString((cache + offset), 4096));
                }

                rangeCacheFinishMtx[cacheIndex].lock();
                auto rangeCount = ++rangeCacheCount[cacheIndex];
                if (rangeCount == 64) {
                    isCacheWritable[cacheIndex] = true;
                    isCacheReadable[cacheIndex] = false;
                    rangeCacheCount[cacheIndex] = 0;
                    rangeCacheFinish[cacheIndex].notify_all();
                }
                rangeCacheFinishMtx[cacheIndex].unlock();
//                lock.unlock();
            }

            //重置参数
            rangeAllFinishMtx.lock();

            int finishCount = ++rangeAllFinishCount;
            if (finishCount == 64) {
                rangeAllFinishCount = 0;
                rangeAllFinishFlag = true;
                readDiskFlag.clear();
                for (int i = 0; i < CACHE_NUM; i++) {
                    readDiskCount[i] = 0;
                    rangeCacheCount[i] = 0;
                    isCacheWritable[i] = true;
                    isCacheReadable[i] = false;
                }

                rangeAllFinish.notify_all();
                rangeAllFinishMtx.unlock();
            } else {
                while (!rangeAllFinishFlag) {
                    rangeAllFinish.wait(rangeAllFinishMtx);
                }
                rangeAllFinishMtx.unlock();
            }
        }

    };
}

#endif //ENGINE_RACE_PENGINE_H