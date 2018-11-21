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
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include <map>
#include "../include/polar_string.h"
#include "../include/engine.h"
#include "key_log.h"
#include "value_log.h"
#include "SortLog.h"
#include "CacheQueue.h"
#include <unordered_map>

#define LOG_NUM 256
#define NUM_PER_SLOT 255000
#define VALUE_LOG_SIZE NUM_PER_SLOT * 4096
#define KEY_LOG_SIZE NUM_PER_SLOT * 8
#define RECOVER_THREAD 64


using namespace std;
using namespace std::chrono;
namespace polar_race {
    static char *prepare() {
        auto buffer = static_cast<char *>(malloc(4096));
        posix_memalign((void **) &buffer, 4096, 4096);
        return buffer;
    }

    static thread_local std::unique_ptr<char> readBuffer(static_cast<char *>(prepare()));

    milliseconds now() {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    }

    class PEngine {

    private:
        KeyLog **keyLogs;
        ValueLog **valueLogs;
        mutex logMutex[LOG_NUM];

        SortLog **sortLogs;
        u_int64_t endKey;

        CacheQueue *cacheQueue;

        atomic_bool readThreadFlag;
        unordered_map<long, int> readThreadIdHash;
        int readThreadId;
        mutex readThreadIdLock;

        milliseconds start;

    public:
        explicit PEngine(const string &path) {
            this->start = now();

            this->keyLogs = static_cast<KeyLog **>(malloc(LOG_NUM * sizeof(KeyLog *)));
            this->valueLogs = static_cast<ValueLog **>(malloc(LOG_NUM * sizeof(ValueLog *)));

            std::ostringstream ss;
            ss << path << "/key-0";
            string filePath = ss.str();

            if (access(filePath.data(), 0) != -1) {
                this->sortLogs = static_cast<SortLog **>(malloc(LOG_NUM * sizeof(SortLog *)));
                for (int i = 0; i < LOG_NUM; i++) {
                    *(keyLogs + i) = new KeyLog(path, i, KEY_LOG_SIZE);
                    *(valueLogs + i) = new ValueLog(path, i, VALUE_LOG_SIZE);
                    *(sortLogs + i) = new SortLog(NUM_PER_SLOT);
                }
                this->cacheQueue = new CacheQueue(this->sortLogs);
                this->readThreadFlag = false;
                readThreadId = 0;

                recover();
                fprintf(stderr, "Open database complete. time spent is %lims\n", (now() - start).count());
            } else {
                for (int i = 0; i < LOG_NUM; i++) {
                    *(keyLogs + i) = new KeyLog(path, i, KEY_LOG_SIZE);
                    *(valueLogs + i) = new ValueLog(path, i, VALUE_LOG_SIZE);
                }
            }

            printf("============================Engine Start!========================\n");
        }

        ~PEngine() {
            for (int i = 0; i < LOG_NUM; i++) {
                delete keyLogs[i];
                delete valueLogs[i];
            }
            delete[] keyLogs;
            delete[] valueLogs;

            if (sortLogs != nullptr) {
                for (int i = 0; i < LOG_NUM; i++)
                    delete sortLogs[i];
                delete[] sortLogs;
                delete cacheQueue;
            }
            fprintf(stderr, "deleting engine, total life is %lims\n", (now() - start).count());
            printf("============================Engine Stop!========================\n");

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

            this->endKey = sortLogs[LOG_NUM - 1]->findKeyByIndex(sortLogs[LOG_NUM - 1]->size() - 1);
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

                printf("====sort %d size %d=====\n",id,sortLog->size());
                id++;
            }
        }

        int getLogId(const char *k) {
//            return ((u_int16_t)((u_int8_t)k[0]) << 2) | ((u_int8_t) k[1] >> 6);
            return (*((u_int8_t *) k));
        }

        void put(const PolarString &key, const PolarString &value) {
            auto logId = getLogId(key.data());
            logMutex[logId].lock();
            valueLogs[logId]->putValue(value.data());
            keyLogs[logId]->putValue(key.data());
            logMutex[logId].unlock();
        }

        RetCode read(const PolarString &key, string *value) {
            auto buffer = readBuffer.get();
            auto logId = getLogId(key.data());
            auto index = sortLogs[logId]->find(*((u_int64_t *) key.data()));

            if (index == -1) {
                return kNotFound;
            } else {
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

            printf("%lu   %lu\n", swapEndian(lowerKey), swapEndian(upperKey));

            if (lowerFlag && upperFlag && sortLogs[0]->size() > 200000) {
                return rangeAll(visitor);
            }

            auto buffer = readBuffer.get();
            if (lowerLogId > upperLogId && !upperFlag) {
                return kInvalidArgument;
            } else {
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
//                    printf("RANGE    %d   %d   %d\n", l, r, logId);
                    range(l, r, sortLog, valueLog, visitor, buffer);
                }
            }
            return kSucc;
        }

        void
        range(int lowerIndex, int upperIndex, SortLog *sortLog, ValueLog *valueLog, Visitor &visitor, char *buffer) {
            for (int i = lowerIndex; i <= upperIndex; i++) {
                auto offset = sortLog->findValueByIndex(i);

                valueLog->readValue(offset, buffer);
                u_int64_t k = sortLog->findKeyByIndex(i);
                visitor.Visit(PolarString(((char *) (&k)), 8), PolarString(buffer, 4096));

            }
        }

        RetCode rangeAll(Visitor &visitor) {
            bool expected = false;
            if (readThreadFlag.compare_exchange_strong(expected, true)) {
                //启动读磁盘线程
                printf("===============start read thread==================\n");
                std::thread readDiskThread = std::thread(&PEngine::readDisk, this);
            }

            long id = syscall(224);

            auto it = readThreadIdHash.find(id);
            if (it == readThreadIdHash.end()) {
                readThreadIdLock.lock();
                readThreadIdHash.insert(make_pair(id, readThreadId));
                printf("range threadId %lu, giveid %d\n", id, readThreadId);
                readThreadId++;
                readThreadIdLock.unlock();
            }
            it = readThreadIdHash.find(id);
            int threadId = it->second;
            printf("hash threadId %lu, giveid %d\n", id, threadId);

            PolarString key;
            PolarString value;
            while (true) {
                cacheQueue->read(threadId, key, value);
                visitor.Visit(key, value);
                if (*((u_int64_t *) key.data()) == this->endKey)
                    break;
            }

            return kSucc;
        }

        void readDisk() {
            u_int16_t logId;
            u_int32_t offset;
            while (true) {
                char *buffer = cacheQueue->getPutBlock(logId, offset);
                if (logId == LOG_NUM) break;
                valueLogs[logId]->readValue(offset, buffer);
                cacheQueue->addRear();
            }
        }

        u_int64_t swapEndian(u_int64_t key) {
            return (((key & 0x00000000000000FF) << 56) |
                    ((key & 0x000000000000FF00) << 40) |
                    ((key & 0x0000000000FF0000) << 24) |
                    ((key & 0x00000000FF000000) << 8) |
                    ((key & 0x000000FF00000000) >> 8) |
                    ((key & 0x0000FF0000000000) >> 24) |
                    ((key & 0x00FF000000000000) >> 40) |
                    ((key & 0xFF00000000000000) >> 56));
        }
    };

}

#endif //ENGINE_RACE_PENGINE_H
