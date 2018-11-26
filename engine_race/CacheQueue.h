//
// Created by 杨煜溟 on 2018/11/20.
//

#ifndef ENGINE_CACHEQUEUECONDITION_H
#define ENGINE_CACHEQUEUECONDITION_H

#include "SortLog.h"
#include "../include/polar_string.h"
#include "../include/engine.h"

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>

#define CACHE_SIZE 100
#define LOG_NUM 256

using namespace std;

namespace polar_race {
    class CacheQueue {
    private:
        u_int32_t rear;

        //每个线程下一个要读取的位置
        u_int32_t readPositions[64]{0};

        u_int64_t *keys;
        char *values;
        std::mutex getBlockMutex;

        SortLog **sortLogs;
        u_int16_t currentSortLog;
        int currentSortIndex;
        int rangeCnt = 0;

        std::mutex putFinishMtx[CACHE_SIZE];
        std::mutex getFinishMtx[CACHE_SIZE];

        std::condition_variable empty[CACHE_SIZE];
        std::condition_variable full[CACHE_SIZE];
        int remain[CACHE_SIZE]{0};
        int real[CACHE_SIZE]{0};

        int totalNum = 0;


    public:
        explicit CacheQueue(SortLog **sortLogs) : sortLogs(sortLogs), rear(0), currentSortLog(0),
                                                           currentSortIndex(0) {
            this->keys = (u_int64_t *) (malloc((CACHE_SIZE) * sizeof(u_int64_t)));
            this->values = (char *) (malloc((CACHE_SIZE) * 4096));
            posix_memalign((void **) &values, 4096, CACHE_SIZE * 4096);

            for (int i = 0; i < CACHE_SIZE; i++)
                real[i] = i - CACHE_SIZE;

            for (int i = 0; i < LOG_NUM; i++) {
                totalNum += sortLogs[i]->size();
            }

            printf("total num: %d\n", totalNum);
        }

        ~CacheQueue() {
            delete keys;
            delete values;
        }

        //64个线程读取下一个缓存块，返回key和cache地址
        void read(Visitor &visitor) {
            int readPositions = 0;
            for (int logId = 0; logId < LOG_NUM; logId++) {
                for (int i = 0, total = sortLogs[logId]->size(); i < total; i++) {

                    int position = readPositions % CACHE_SIZE;
                    if (remain[position] == 0 || readPositions != real[position]) {
                        unique_lock<mutex> lck(putFinishMtx[position]);
                        while (remain[position] == 0 || readPositions != real[position])
                            full[position].wait(lck);
                        lck.unlock();
                    }

                    u_int64_t k = sortLogs[logId]->findKeyByIndex(i);
                    char *v = values + (position * 4096);
                    visitor.Visit(PolarString(((char *) (&k)), 8), PolarString(v, 4096));
                    readPositions++;

                    unique_lock<mutex> lck(getFinishMtx[position]);
                    remain[position]--;
                    if (remain[position] == 0)
                        empty[position].notify_all();
                    lck.unlock();
                }
            }
        }


        // 读磁盘线程获取下一个写缓存块，以及文件位置，logId == LOG_NUM 说明读取结束
        char *getPutBlock(u_int16_t &logId, u_int32_t &offset, u_int32_t &realNum) {

            getBlockMutex.lock();
            int position = rear % CACHE_SIZE;
            realNum = rear;
            getNextKeyOffset(logId, offset);
            rear = (rear + 1) % totalNum;
            getBlockMutex.unlock();

            if (remain[position] > 0 || (real[position] + CACHE_SIZE) % totalNum != realNum) {
                unique_lock<mutex> lck(getFinishMtx[position]);
                while (remain[position] > 0 || (real[position] + CACHE_SIZE) % totalNum != realNum) {
                    empty[position].wait(lck);
                }
                lck.unlock();
            }

            char *valueCache = values + ((position) * 4096);
            return valueCache;
        }


        //应该由读磁盘线程来操作！！！
        void putBlockFinished(u_int32_t realNum) {
            int position = realNum % CACHE_SIZE;
            unique_lock<mutex> lck(putFinishMtx[position]);
            remain[position] = 64;
            real[position] = realNum;
            full[position].notify_all();
        }


        void getNextKeyOffset(u_int16_t &logId, u_int32_t &offset) {

            if (currentSortIndex == sortLogs[currentSortLog]->size()) {
                if (currentSortLog == LOG_NUM - 1)
                    rangeCnt++;

                currentSortLog = (u_int16_t) ((currentSortLog + 1) % LOG_NUM);
                currentSortIndex = 0;
            }

            logId = currentSortLog;
            offset = sortLogs[currentSortLog]->findValueByIndex(currentSortIndex);

            currentSortIndex++;

            if (rangeCnt == 2)
                logId = LOG_NUM;
        }

    };
}

#endif
//ENGINE_CACHEQUEUECONDITION_H
