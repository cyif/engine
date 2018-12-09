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
#include "KVFiles.h"
#include "KeyValueLog.h"
#include "params.h"

using namespace std;
using namespace std::chrono;

namespace polar_race {

    milliseconds now() {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    }

    static thread_local std::unique_ptr<char> readBuffer(static_cast<char *> (memalign((size_t) getpagesize(), 4096)));

    KeyValueLog *keyValueLogs[LOG_NUM];
    KVFiles *kvFiles[FILE_NUM];
    SortLog **sortLogs;

    u_int64_t *sortKeysArray;
    u_int16_t *sortValuesArray;

    PMutex logMutex[LOG_NUM];

    char *valueCache;
    char *reserveCache;
    std::atomic_flag readDiskFlag = ATOMIC_FLAG_INIT;

    PCond rangeCacheFinish[CACHE_NUM];
    std::atomic<int> rangeCacheCount[CACHE_NUM];

    PCond readDiskFinish[CACHE_NUM];

    bool isCacheReadable[CACHE_NUM];
    bool isCacheWritable[CACHE_NUM];
    int currentCacheLogId[CACHE_NUM];

    PMutex readDiskLogIdMtx;
    int readDiskLogId = 0;
    int rangeAllCount = 0;

    class PEngine {

    private:

        milliseconds start;
        int totalNum = 0;
        bool enlarge = false;

    public:
        explicit PEngine(const string &path) {
            this->start = now();
            milliseconds t0 = this->start;

            // init
            std::ostringstream ss;
            ss << path << "/value-0";
            string filePath = ss.str();

            valueCache = nullptr;
            sortLogs = nullptr;

            int num_log_per_file = LOG_NUM / FILE_NUM;

            if (access(filePath.data(), 0) != -1) {

                sortLogs = static_cast<SortLog **>(malloc(LOG_NUM * sizeof(SortLog *)));
                sortKeysArray = (u_int64_t *) malloc(SORT_LOG_SIZE * LOG_NUM * sizeof(u_int64_t));
                sortValuesArray = (u_int16_t *) malloc(SORT_LOG_SIZE * LOG_NUM * sizeof(u_int16_t));

                valueCache = static_cast<char *> (memalign((size_t) getpagesize(), CACHE_SIZE * CACHE_NUM));
                reserveCache = valueCache + (ACTIVE_CACHE_NUM * CACHE_SIZE);

                for (int fileId = 0; fileId < FILE_NUM; fileId++) {
                    kvFiles[fileId] = new KVFiles(path, fileId,
                                                  true,
                                                  VALUE_LOG_SIZE * num_log_per_file,
                                                  KEY_LOG_SIZE * num_log_per_file,
                                                  BLOCK_SIZE * num_log_per_file);
                }

                for (int logId = 0; logId < LOG_NUM; logId++) {
                    int fileId = logId % FILE_NUM;
                    int slotId = logId / FILE_NUM;
                    keyValueLogs[logId] = new KeyValueLog(path, logId,
                                                          kvFiles[fileId]->getValueFd(),
                                                          slotId * VALUE_LOG_SIZE,
                                                          kvFiles[fileId]->getBlockBuffer() + slotId * BLOCK_SIZE,
                                                          kvFiles[fileId]->getKeyBuffer() + slotId * NUM_PER_SLOT);
                    sortLogs[logId] = new SortLog(sortKeysArray + SORT_LOG_SIZE * logId,
                                                  sortValuesArray + SORT_LOG_SIZE * logId);
                }


                for (int i = 0; i < CACHE_NUM; i++) {
                    rangeCacheCount[i] = 0;
                    isCacheReadable[i] = false;
                    isCacheWritable[i] = true;
                    currentCacheLogId[i] = -1;
                }

                printf("Open files complete. time spent is %lims\n", (now() - t0).count());
                milliseconds t1 = now();


                //64线程读keylog
                std::thread t[RECOVER_THREAD];
                for (int i = 0; i < RECOVER_THREAD; i++) {
                    t[i] = std::thread([i, this] {
                        u_int64_t k;
                        for (int logId = i; logId < LOG_NUM; logId += RECOVER_THREAD) {
                            while (keyValueLogs[logId]->getKey(k))
                                sortLogs[logId]->put(k);
                            keyValueLogs[logId]->recover((size_t) sortLogs[logId]->size());
                        }
                    });
                }

                for (auto &i : t) {
                    i.join();
                }

                printf("read keylogs complete. time spent is %lims\n", (now() - t1).count());
                milliseconds t2 = now();


                //4线程读cache
                std::thread t_cache[PREPARE_CACHE_NUM];

                for (int i = 0; i < PREPARE_CACHE_NUM; i++) {
                    t_cache[i] = std::thread([i, this] {
                        auto s = now();
                        auto cache = reserveCache + i * CACHE_SIZE;
                        keyValueLogs[i]->readValue(0, cache, (size_t) CACHE_SIZE);
                        printf("Finish Read Reserve Cache : %d,  %lims\n", i, (now() - s).count());
                    });
                }

                //64线程排序
                for (int i = 0; i < RECOVER_THREAD; i++) {
                    t[i] = std::thread([i, this] {
                        u_int64_t k;
                        for (int logId = i; logId < LOG_NUM; logId += RECOVER_THREAD) {
                            sortLogs[logId]->quicksort();
                        }
                    });
                }

                for (auto &i : t) {
                    i.join();
                }

                for (int i = 0; i < PREPARE_CACHE_NUM; i++) t_cache[i].join();

                printf("sort and read cache complete. time spent is %lims\n", (now() - t2).count());

                for (int logId = 0; logId < LOG_NUM; logId++)
                    totalNum += sortLogs[logId]->size();

                if (totalNum < RANGE_THRESHOLD) {
                    enlarge = true;
                }

            } else {
                for (int fileId = 0; fileId < FILE_NUM; fileId++) {
                    kvFiles[fileId] = new KVFiles(path, fileId,
                                                  false,
                                                  VALUE_LOG_SIZE * num_log_per_file,
                                                  KEY_LOG_SIZE * num_log_per_file,
                                                  BLOCK_SIZE * num_log_per_file);
                }

                for (int logId = 0; logId < LOG_NUM; logId++) {
                    int fileId = logId % FILE_NUM;
                    int slotId = logId / FILE_NUM;
                    keyValueLogs[logId] = new KeyValueLog(path, logId,
                                                          kvFiles[fileId]->getValueFd(),
                                                          slotId * VALUE_LOG_SIZE,
                                                          kvFiles[fileId]->getBlockBuffer() + slotId * BLOCK_SIZE,
                                                          kvFiles[fileId]->getKeyBuffer() + slotId * NUM_PER_SLOT);
                }
            }
            printf("Open database complete. time spent is %lims\n", (now() - start).count());
        }

        ~PEngine() {
//            printf("deleting engine, total life is %lims\n", (now() - start).count());

            std::thread t[RECOVER_THREAD];
            for (int i = 0; i < RECOVER_THREAD; i++) {
                t[i] = std::thread([i, this] {
                    for (int logId = i; logId < LOG_NUM; logId += RECOVER_THREAD) {
                        delete keyValueLogs[logId];
                    }
                });
            }

            for (auto &i : t) {
                i.join();
            }

            for (auto kvFilesi : kvFiles)
                delete kvFilesi;


            if (sortLogs != nullptr) {
                for (int i = 0; i < LOG_NUM; i++)
                    delete sortLogs[i];
                free(sortLogs);

                free(sortKeysArray);
                free(sortValuesArray);

                if (valueCache != nullptr) {
                    free(valueCache);
                }
            }
//            printf("Finish deleting engine, total life is %lims\n", (now() - start).count());
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
                if (logId < PREPARE_CACHE_NUM && !enlarge) {
                    value->assign(reserveCache + logId * CACHE_SIZE + (index << 12), 4096);
                } else {
                    auto buffer = readBuffer.get();
                    keyValueLogs[logId]->readValue(index, buffer);
                    value->assign(buffer, 4096);
                }
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

            if (lower == "" && upper == "" && (totalNum > RANGE_THRESHOLD)) {
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
        range(int lowerIndex, int upperIndex, SortLog *sortLog, KeyValueLog *keyValueLog, Visitor &visitor,
              char *buffer) {
            for (int i = lowerIndex; i <= upperIndex; i++) {
                auto offset = sortLog->findValueByIndex(i);
                if (offset >= 0) {
                    keyValueLog->readValue(offset, buffer);
                    u_int64_t k = __builtin_bswap64(sortLog->findKeyByIndex(i));
                    visitor.Visit(PolarString(((char *) (&k)), 8), PolarString(buffer, 4096));
                }
            }
        }


        void readDisk() {
//            printf("Start Read Disk Thread!\n");

            int cacheIndex;
            char *cache;

            while (true) {

                readDiskLogIdMtx.lock();

                if (rangeAllCount == MAX_RANGE_COUNT) {
                    readDiskFlag.clear();
                    readDiskLogIdMtx.unlock();
                    break;
                }

                int logId = readDiskLogId;
                readDiskLogId++;

                if (readDiskLogId >= LOG_NUM) {
                    readDiskLogId = 0;
                    rangeAllCount++;
                }

                if (logId < RESERVE_CACHE_NUM) {
                    cacheIndex = logId + ACTIVE_CACHE_NUM;
                    readDiskLogIdMtx.unlock();

                    if (!isCacheReadable[cacheIndex]) {
                        isCacheWritable[cacheIndex] = false;
                        currentCacheLogId[cacheIndex] = logId;

                        if (logId >= PREPARE_CACHE_NUM) {
                            cache = valueCache + cacheIndex * CACHE_SIZE;;
                            keyValueLogs[logId]->readValue(0, cache, (size_t) keyValueLogs[logId]->size());
                        }

                        readDiskFinish[cacheIndex].lock();
                        isCacheReadable[cacheIndex] = true;
                        readDiskFinish[cacheIndex].notify_all();
                        readDiskFinish[cacheIndex].unlock();
                    }

                } else {
                    cacheIndex = logId % ACTIVE_CACHE_NUM;

                    if (!isCacheWritable[cacheIndex]) {
                        rangeCacheFinish[cacheIndex].lock();
                        while (!isCacheWritable[cacheIndex]) {
//                            printf("wait for range log: %d , cache index: %d, range round: %d \n", logId, cacheIndex, rangeAllCount);
                            rangeCacheFinish[cacheIndex].wait();
                        }
                        rangeCacheFinish[cacheIndex].unlock();
                    }

                    isCacheWritable[cacheIndex] = false;
                    currentCacheLogId[cacheIndex] = logId;
                    cache = valueCache + cacheIndex * CACHE_SIZE;

                    readDiskLogIdMtx.unlock();

                    keyValueLogs[logId]->readValue(0, cache, (size_t) keyValueLogs[logId]->size());
                    readDiskFinish[cacheIndex].lock();
                    isCacheReadable[cacheIndex] = true;
                    readDiskFinish[cacheIndex].notify_all();
                    readDiskFinish[cacheIndex].unlock();
                }
            }
        }


        void rangeAll(Visitor &visitor) {
            if (!readDiskFlag.test_and_set()) {

                std::thread t[RECOVER_THREAD];
                for (int i = 0; i < RECOVER_THREAD; i++) {
                    t[i] = std::thread([i, this] {
                        for (int logId = i; logId < LOG_NUM; logId += RECOVER_THREAD) {
                            sortLogs[logId]->swap();
                        }
                    });
                }

                for (auto &i : t) {
                    i.join();
                }

                for (int i = 0; i < READDISK_THREAD; i++) {
                    std::thread t = std::thread(&PEngine::readDisk, this);
                    t.detach();
                }
            }

            for (int logId = 0; logId < RESERVE_CACHE_NUM; logId++) {
                auto cacheIndex = logId + ACTIVE_CACHE_NUM;
                if (!isCacheReadable[cacheIndex]) {
                    readDiskFinish[cacheIndex].lock();
                    while (!isCacheReadable[cacheIndex]) {
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
            }

            for (int logId = RESERVE_CACHE_NUM; logId < LOG_NUM; logId++) {

                // 等待读磁盘线程读完当前valueLog
                auto cacheIndex = logId % ACTIVE_CACHE_NUM;
                if (!isCacheReadable[cacheIndex] || currentCacheLogId[cacheIndex] != logId) {
                    readDiskFinish[cacheIndex].lock();
                    while (!isCacheReadable[cacheIndex] || currentCacheLogId[cacheIndex] != logId) {
                        readDiskFinish[cacheIndex].wait();
                    }
                    readDiskFinish[cacheIndex].unlock();
                }

                auto cache = valueCache + cacheIndex * CACHE_SIZE;
                auto sortLog = sortLogs[logId];

                for (int i = 0, total = sortLog->size(); i < total; i++) {
                    auto k = sortLog->findKeyByIndex(i);
                    auto offset = sortLog->findValueByIndex(i) << 12;
                    visitor.Visit(PolarString(((char *) (&k)), 8), PolarString((cache + offset), 4096));
                }

                if (++rangeCacheCount[cacheIndex] == 64) {
                    rangeCacheFinish[cacheIndex].lock();
                    isCacheWritable[cacheIndex] = true;
                    isCacheReadable[cacheIndex] = false;
                    rangeCacheCount[cacheIndex] = 0;
                    rangeCacheFinish[cacheIndex].notify_all();
                    rangeCacheFinish[cacheIndex].unlock();
                }
            }
        }

    };
}

#endif //ENGINE_RACE_PENGINE_H