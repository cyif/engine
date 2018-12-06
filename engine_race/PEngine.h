#ifndef ENGINE_RACE_PENGINE_H
#define ENGINE_RACE_PENGINE_H

#include <malloc.h>
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
#include "SortLog.h"
#include "ThreadPool.h"
#include "KVFiles.h"
#include "KeyValueLog.h"
#include "SortArray.h"
#include "params.h"

using namespace std;
using namespace std::chrono;

namespace polar_race {

    milliseconds now() {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    }

    static thread_local std::unique_ptr<char> readBuffer(static_cast<char *> (memalign((size_t) getpagesize(), 4096)));

    class PEngine {

    private:
        KeyValueLog *(keyValueLogs[LOG_NUM]);
        KVFiles *(kvFiles[FILE_NUM]);
        SortLog **sortLogs;
        SortArray *sortArray;
        PMutex logMutex[LOG_NUM];

        ThreadPool *readDiskThreadPool;
        char *valueCache;
        std::atomic_flag readDiskFlag = ATOMIC_FLAG_INIT;

        PCond rangeCacheFinish[CACHE_NUM];
        int rangeCacheCount[CACHE_NUM];

        PCond readDiskFinish[CACHE_NUM];

        bool isCacheReadable[CACHE_NUM];
        bool isCacheWritable[CACHE_NUM];
        int currentCacheLogId[CACHE_NUM];

        milliseconds start;

    public:
        explicit PEngine(const string &path) {
            this->start = now();
            milliseconds t0 = this->start;

            // init
            //value cache file
            std::ostringstream ss;
            ss << path << "/value-0";
            string filePath = ss.str();

            int num_log_per_file = LOG_NUM / FILE_NUM;

            if (access(filePath.data(), 0) != -1) {

                this->sortLogs = static_cast<SortLog **>(malloc(LOG_NUM * sizeof(SortLog *)));
                this->valueCache = nullptr;

                for (int fileId = 0; fileId < FILE_NUM; fileId++) {
                    kvFiles[fileId] = new KVFiles(path, fileId,
                                                  VALUE_LOG_SIZE * num_log_per_file,
                                                  KEY_LOG_SIZE * num_log_per_file,
                                                  BLOCK_SIZE * num_log_per_file);
                }

                printf("Open files complete. time spent is %lims\n", (now() - t0).count());
                milliseconds t1 = now();

                for (int logId = 0; logId < LOG_NUM; logId++) {
                    int fileId = logId % FILE_NUM;
                    int slotId = logId / FILE_NUM;
                    keyValueLogs[logId] = new KeyValueLog(path, logId,
                                                          this->kvFiles[fileId]->getValueFd(),
                                                          slotId * VALUE_LOG_SIZE,
                                                          this->kvFiles[fileId]->getBlockBuffer() + slotId * BLOCK_SIZE,
                                                          this->kvFiles[fileId]->getKeyBuffer() + slotId * NUM_PER_SLOT);
                }

                printf("Open KeyValueLogs complete. time spent is %lims\n", (now() - t1).count());
                milliseconds t2 = now();

                sortArray = new SortArray();

                for (int logId = 0; logId < LOG_NUM; logId++) {
                    int fileId = logId % FILE_NUM;
                    int slotId = logId / FILE_NUM;
                    sortLogs[logId] = new SortLog(sortArray->getKeyArray(logId), sortArray->getValueArray(logId));
                }

                printf("Open sortlogs complete. time spent is %lims\n", (now() - t2).count());
                milliseconds t3 = now();

                std::thread t[RECOVER_THREAD];
                for (int i = 0; i < RECOVER_THREAD; i++) {
                    t[i] = std::thread([i, num_log_per_file, path, this] {
                        u_int64_t k;
                        for (int logId = i; logId < LOG_NUM; logId += RECOVER_THREAD) {
                            while (keyValueLogs[logId]->getKey(k))
                                sortLogs[logId]->put(k);
                            sortLogs[logId]->quicksort();
                            keyValueLogs[logId]->setKeyBufferPosition((size_t) sortLogs[logId]->size());
                            keyValueLogs[logId]->recover((size_t) sortLogs[logId]->size());
                        }
                    });
                }
                for (auto &i : t) {
                    i.join();
                }

                printf("Recover sortlogs complete. time spent is %lims\n", (now() - t3).count());

            }

            else {
                this->sortLogs = nullptr;
                this->sortArray = nullptr;
                this->valueCache = nullptr;
                std::thread t[RECOVER_THREAD];
                for (int i = 0; i < RECOVER_THREAD; i++) {
                    t[i] = std::thread([i, num_log_per_file, path, this] {
                        for (int fileId = i; fileId < FILE_NUM; fileId += RECOVER_THREAD) {
                            kvFiles[fileId] = new KVFiles(path, fileId,
                                                          VALUE_LOG_SIZE * num_log_per_file,
                                                          KEY_LOG_SIZE * num_log_per_file,
                                                          BLOCK_SIZE * num_log_per_file);
                        }
                        for (int logId = i; logId < LOG_NUM; logId += RECOVER_THREAD) {
                            int fileId = logId % FILE_NUM;
                            int slotId = logId / FILE_NUM;
                            keyValueLogs[logId] = new KeyValueLog(path, logId,
                                                                  this->kvFiles[fileId]->getValueFd(),
                                                                  slotId * VALUE_LOG_SIZE,
                                                                  this->kvFiles[fileId]->getBlockBuffer() + slotId * BLOCK_SIZE,
                                                                  this->kvFiles[fileId]->getKeyBuffer() + slotId * NUM_PER_SLOT);
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

            for (auto keyValueLogsi : keyValueLogs)
                delete keyValueLogsi;

            for (auto kvFilesi : kvFiles)
                delete kvFilesi;


            if (sortLogs != nullptr && sortArray != nullptr) {
                for (int i = 0; i < LOG_NUM; i++)
                    delete sortLogs[i];
                free(sortLogs);

                free(sortArray);

                if (valueCache != nullptr){
                    free(valueCache);
                }
            }
            printf("Finish deleting engine, total life is %lims\n", (now() - start).count());
        }

        static inline int getLogId(const char *k) {
            return ((u_int16_t) ((u_int8_t) k[0]) << 4) | ((u_int8_t) k[1] >> 4);
        }

        void put(const PolarString &key, const PolarString &value) {
            auto logId = getLogId(key.data());
            logMutex[logId].lock();
            keyValueLogs[logId]->putValueKey(value.data(), key.data());
            logMutex[logId].unlock();
        }

        RetCode read(const PolarString &key, string *value) {
            auto logId = getLogId(key.data());
            auto index = sortLogs[logId]->find(*((u_int64_t *) key.data()));

            if (index == -1) {
                return kNotFound;
            } else {
                auto buffer = readBuffer.get();
                keyValueLogs[logId]->readValue(index, buffer);
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
//
//            if (lower == "" && upper == "") {
//                rangeAll(visitor);
//                return kSucc;
//            }

            if (lower == "" && upper == "" && (sortLogs[0]->size() > 12000)) {
                rangeAll(visitor);
                return kSucc;
            }

            if (lowerLogId > upperLogId && !upperFlag) {
                return kInvalidArgument;
            } else {
                auto buffer = readBuffer.get();

                for (int logId = lowerLogId; logId <= upperLogId; logId++) {
                    SortLog *sortLog = sortLogs[logId];
                    KeyValueLog *keyValueLog = keyValueLogs[logId];

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
                    range(l, r, sortLog, keyValueLog, visitor, buffer);
                }
            }
            return kSucc;
        }

        void
        range(int lowerIndex, int upperIndex, SortLog *sortLog, KeyValueLog *keyValueLog, Visitor &visitor, char *buffer) {
            for (int i = lowerIndex; i <= upperIndex; i++) {
                auto offset = sortLog->findValueByIndex(i);
                if (offset >= 0) {
                    keyValueLog->readValue(offset, buffer);
                    u_int64_t k = sortLog->findKeyByIndex(i);
                    visitor.Visit(PolarString(((char *) (&k)), 8), PolarString(buffer, 4096));
                }
            }
        }

        void readDisk() {
            int logId = 0;
            int rangeAllCount = 0;
            while (true) {
                auto cacheIndex = logId % CACHE_NUM;

                if (!isCacheWritable[cacheIndex]) {
                    //等待获取可用的cache
                    rangeCacheFinish[cacheIndex].lock();
                    while (!isCacheWritable[cacheIndex]) {
                        rangeCacheFinish[cacheIndex].wait();
                    }
                    rangeCacheFinish[cacheIndex].unlock();
                }

                isCacheWritable[cacheIndex] = false;
                currentCacheLogId[cacheIndex] = logId;
                auto cache = valueCache + cacheIndex * CACHE_SIZE;

                readDiskThreadPool->enqueue([cacheIndex, cache, logId, this] {
                    keyValueLogs[logId]->readValue(0, cache, (size_t) keyValueLogs[logId]->size());
                    readDiskFinish[cacheIndex].lock();
                    isCacheReadable[cacheIndex] = true;
                    readDiskFinish[cacheIndex].notify_all();
                    readDiskFinish[cacheIndex].unlock();
                });

                logId++;
                if (logId >= LOG_NUM) {
                    logId = 0;
                    rangeAllCount++;
                    if (rangeAllCount == MAX_RANGE_COUNT) {
                        readDiskFlag.clear();
                        break;
                    }
                }
            }
        }


        void rangeAll(Visitor &visitor) {
            if (!readDiskFlag.test_and_set()) {
                this->valueCache = static_cast<char *> (memalign((size_t) getpagesize(), CACHE_SIZE * CACHE_NUM));
                readDiskThreadPool = new ThreadPool(READDISK_THREAD);
                for (int i = 0; i < CACHE_NUM; i++) {
                    rangeCacheCount[i] = 0;
                    isCacheReadable[i] = false;
                    isCacheWritable[i] = true;
                    currentCacheLogId[i] = -1;
                }
                std::thread t = std::thread(&PEngine::readDisk, this);
                t.detach();
            }

            for (int logId = 0; logId < LOG_NUM; logId++) {

                // 等待读磁盘线程读完当前valueLog
                auto cacheIndex = logId % CACHE_NUM;
                if (!isCacheReadable[cacheIndex] || currentCacheLogId[cacheIndex] != logId) {
                    readDiskFinish[cacheIndex].lock();
                    while (!isCacheReadable[cacheIndex] || currentCacheLogId[cacheIndex] != logId) {
                        readDiskFinish[cacheIndex].wait();
                    }
                    readDiskFinish[cacheIndex].unlock();
                }

                auto cache = valueCache + cacheIndex * CACHE_SIZE;

                for (int i = 0, total = sortLogs[logId]->size(); i < total; i++) {
                    auto k = sortLogs[logId]->findKeyByIndex(i);
                    auto offset = sortLogs[logId]->findValueByIndex(i) << 12;
                    visitor.Visit(PolarString(((char *) (&k)), 8), PolarString((cache + offset), 4096));
                }

                rangeCacheFinish[cacheIndex].lock();
                auto rangeCount = ++rangeCacheCount[cacheIndex];
                if (rangeCount == 64) {
                    isCacheWritable[cacheIndex] = true;
                    isCacheReadable[cacheIndex] = false;
                    rangeCacheCount[cacheIndex] = 0;
                    rangeCacheFinish[cacheIndex].notify_all();
                }
                rangeCacheFinish[cacheIndex].unlock();
            }
        }

    };
}

#endif //ENGINE_RACE_PENGINE_H