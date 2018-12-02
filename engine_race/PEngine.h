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
        KeyValueLog **keyValueLogs;
        SortLog **sortLogs;
        KVFiles **kvFiles;
        std::mutex logMutex[LOG_NUM];

        char *valueCache;
        ThreadPool *readDiskThreadPool;
        std::atomic_flag readDiskFlag = ATOMIC_FLAG_INIT;

        std::condition_variable_any rangeCacheFinish[CACHE_NUM];
        std::mutex rangeCacheFinishMtx[CACHE_NUM];
        int rangeCacheCount[CACHE_NUM] = {0};

        std::condition_variable_any readDiskFinish[CACHE_NUM];
        std::mutex readDiskFinishMtx[CACHE_NUM];
//        int readDiskCount[CACHE_NUM] = {0};

        bool isCacheReadable[CACHE_NUM];
        bool isCacheWritable[CACHE_NUM];
        int currentCacheLogId[CACHE_NUM];

        milliseconds start;

        int cacheFd;
        char *cacheBuffer;
        size_t cacheFileSize;

    public:
        explicit PEngine(const string &path) {
            this->start = now();
            // init

            this->keyValueLogs = static_cast<KeyValueLog **>(malloc(LOG_NUM * sizeof(KeyValueLog *)));
            this->kvFiles = static_cast<KVFiles **>(malloc(FILE_NUM * sizeof(KVFiles *)));

            std::ostringstream cfp;
            cfp << path << "/value-cache";
            //value cache file
            this->cacheFd = open(cfp.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
            fallocate(this->cacheFd, 0, 0, BLOCK_SIZE * LOG_NUM);
            this->cacheBuffer = static_cast<char *>(mmap(nullptr, BLOCK_SIZE * LOG_NUM, PROT_READ | PROT_WRITE,
                                                         MAP_SHARED | MAP_POPULATE, this->cacheFd, 0));

            std::ostringstream ss;
            ss << path << "/value-0";
            string filePath = ss.str();

            int num_log_per_file = LOG_NUM / FILE_NUM;

            if (access(filePath.data(), 0) != -1) {

                this->sortLogs = static_cast<SortLog **>(malloc(LOG_NUM * sizeof(SortLog *)));

                std::thread t[RECOVER_THREAD];
                for (int i = 0; i < RECOVER_THREAD; i++) {
                    t[i] = std::thread([i, num_log_per_file, path, this] {

                        for (int fileId = i; fileId < FILE_NUM; fileId += RECOVER_THREAD) {
                            *(kvFiles + fileId) = new KVFiles(path, fileId, VALUE_LOG_SIZE * num_log_per_file,
                                                              KEY_LOG_SIZE * num_log_per_file);
                        }


                        for (int logId = i; logId < LOG_NUM; logId += RECOVER_THREAD) {
                            sortLogs[logId] = new SortLog();

                            int fileId = logId % FILE_NUM;
                            int slotId = logId / FILE_NUM;

                            int valueFd = this->kvFiles[fileId]->getValueFd();
//                            char *cacheBuffer = this->kvFiles[fileId]->getCacheBuffer() + slotId * BLOCK_SIZE;
                            char *cacheBuffer = this->cacheBuffer + BLOCK_SIZE * logId;
                            size_t globalOffset = slotId * VALUE_LOG_SIZE;
                            u_int64_t *keyBuffer = this->kvFiles[fileId]->getKeyBuffer() + slotId * NUM_PER_SLOT;

                            keyValueLogs[logId] = new KeyValueLog(valueFd, globalOffset, cacheBuffer, keyBuffer);

                            KeyValueLog *keyValueLog = keyValueLogs[logId];
                            SortLog *sortLog = sortLogs[logId];

                            u_int64_t k;
                            u_int32_t cnt = 0;

                            while (keyValueLog->getKey(k))
                                sortLog->put(k, cnt++);

                            sortLog->quicksort();
                            keyValueLog->setKeyBufferPosition(cnt);
                            keyValueLog->recover(cnt);
                        }
                    });
                }

                for (auto &i : t) {
                    i.join();
                }

                this->valueCache = static_cast<char *> (memalign((size_t) getpagesize(), CACHE_SIZE * CACHE_NUM));

                for (int i = 0; i < CACHE_NUM; i++) {
                    isCacheReadable[i] = false;
                    isCacheWritable[i] = true;
                    currentCacheLogId[i] = -1;
                }
                readDiskThreadPool = new ThreadPool(READDISK_THREAD);

            } else {
                this->sortLogs = nullptr;

                std::thread t[RECOVER_THREAD];
                for (int i = 0; i < RECOVER_THREAD; i++) {
                    t[i] = std::thread([i, num_log_per_file, path, this] {

                        for (int fileId = i; fileId < FILE_NUM; fileId += RECOVER_THREAD) {
                            *(kvFiles + fileId) = new KVFiles(path, fileId, VALUE_LOG_SIZE * num_log_per_file,
                                                              KEY_LOG_SIZE * num_log_per_file);
                        }

                        for (int logId = i; logId < LOG_NUM; logId += RECOVER_THREAD) {

                            int fileId = logId % FILE_NUM;
                            int slotId = logId / FILE_NUM;

                            int valueFd = kvFiles[fileId]->getValueFd();
//                            char *cacheBuffer = kvFiles[fileId]->getCacheBuffer() + slotId * BLOCK_SIZE;
                            char *cacheBuffer = this->cacheBuffer + BLOCK_SIZE * logId;
                            size_t globalOffset = slotId * VALUE_LOG_SIZE;
                            u_int64_t *keyBuffer = kvFiles[fileId]->getKeyBuffer() + slotId * NUM_PER_SLOT;

                            keyValueLogs[logId] = new KeyValueLog(valueFd, globalOffset, cacheBuffer, keyBuffer);
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

            for (int logId = 0; logId < LOG_NUM; logId++) {
                delete keyValueLogs[logId];
            }
            delete[] keyValueLogs;

            for (int fileId = 0; fileId < FILE_NUM; fileId++)
                delete kvFiles[fileId];
            delete[] kvFiles;

            munmap(cacheBuffer, BLOCK_SIZE * LOG_NUM);
            close(cacheFd);

            if (sortLogs != nullptr) {
                for (int i = 0; i < LOG_NUM; i++)
                    delete sortLogs[i];
                delete[] sortLogs;
                free(valueCache);
            }
            printf("Finish deleting engine, total life is %lims\n", (now() - start).count());
        }

        static inline int getLogId(const char *k) {
            return ((u_int16_t) ((u_int8_t) k[0]) << 4) | ((u_int8_t) k[1] >> 4);
//            return (((u_int16_t) ((u_int8_t) k[0])) << 1) | ((u_int8_t) k[1] >> 7);
//            return ((u_int16_t) ((u_int8_t) k[0]) << 2) | ((u_int8_t) k[1] >> 6);
//            return (*((u_int8_t *) k));
        }

        void put(const PolarString &key, const PolarString &value) {
            auto logId = getLogId(key.data());
            logMutex[logId].lock();
            keyValueLogs[logId]->putValue(value.data());
            keyValueLogs[logId]->putKey(key.data());
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
            printf("Start Read Disk Thread!\n");
            int logId = 0;
            int rangeAllCount = 0;
            while (true) {

                auto cacheIndex = logId % CACHE_NUM;

                if (!isCacheWritable[cacheIndex]) {
                    //等待获取可用的cache
//                    printf("Cache is not writable. LogId : %d,  CacheIndex %d, ThreadId %ld\n", logId, cacheIndex, gettidv1());
                    rangeCacheFinishMtx[cacheIndex].lock();
                    while (!isCacheWritable[cacheIndex]) {
//                        printf("wait for range log: %d \n",logId);
                        rangeCacheFinish[cacheIndex].wait(rangeCacheFinishMtx[cacheIndex]);
                    }
                    rangeCacheFinishMtx[cacheIndex].unlock();
                }

                isCacheWritable[cacheIndex] = false;
                currentCacheLogId[cacheIndex] = logId;
                auto cache = valueCache + cacheIndex * CACHE_SIZE;

                    readDiskThreadPool->enqueue([cacheIndex, cache, logId, this] {
                        keyValueLogs[logId]->readValue(0, cache, (size_t) keyValueLogs[logId]->size());
                        readDiskFinishMtx[cacheIndex].lock();
                        isCacheReadable[cacheIndex] = true;
                        readDiskFinish[cacheIndex].notify_all();
                        readDiskFinishMtx[cacheIndex].unlock();
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
        }

    };
}

#endif //ENGINE_RACE_PENGINE_H