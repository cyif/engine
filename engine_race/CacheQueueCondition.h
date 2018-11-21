//
// Created by 杨煜溟 on 2018/11/20.
//

#ifndef ENGINE_CACHEQUEUECONDITION_H
#define ENGINE_CACHEQUEUECONDITION_H

#include "SortLog.h"
#include "../include/polar_string.h"

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>

#define CACHE_SIZE 60000
#define LOG_NUM 256

using namespace std;

namespace polar_race {
    class CacheQueueCondition {
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

        std::mutex mtx[CACHE_SIZE];
        std::condition_variable empty[CACHE_SIZE];
        std::condition_variable full[CACHE_SIZE];
        int remain[CACHE_SIZE]{0};


    public:
        explicit CacheQueueCondition(SortLog **sortLogs) : sortLogs(sortLogs), rear(0), currentSortLog(0),
                                                  currentSortIndex(0) {
            this->keys = (u_int64_t *) (malloc((CACHE_SIZE) * sizeof(u_int64_t)));
            this->values = (char *) (malloc((CACHE_SIZE) * 4096));
            posix_memalign((void **) &values, 4096, CACHE_SIZE * 4096);
        }

        ~CacheQueueCondition(){
            delete keys;
            delete values;
        }
        u_int32_t getRealQueuePosition(u_int32_t &position) {
            return position % CACHE_SIZE;
        }

        //64个线程读取下一个缓存块，返回key和cache地址
        void read(int &threadId, PolarString & key, PolarString & value) {

            u_int32_t position = getRealQueuePosition(readPositions[threadId]);

            unique_lock <mutex> lck(mtx[position]);
            while (remain[position] == 0 || readPositions[threadId] == rear) {
//                printf("wait %d\n",position);
//                printf("remain: %d\n",remain[position]);
                full[position].wait(lck);
            }

            key = PolarString((char *)(&keys[position]), 8);
            value = PolarString(values + (position * 4096), 4096);
            readPositions[threadId]++;

            remain[position]--;
            if (remain[position] == 0)
                empty[position].notify_one();

        }

        // 读磁盘线程获取下一个写缓存块，以及文件位置，logId == LOG_NUM 说明读取结束
        char* getPutBlock(u_int16_t &logId, u_int32_t &offset, u_int32_t & position) {
//            getBlockMutex.lock();

            position = getRealQueuePosition(rear);
            unique_lock <mutex> lck(mtx[position]);

            while (remain[position] > 0)
                empty[position].wait(lck);

            getNextKeyOffset(logId, keys[position], offset);
            char* valueCache = values + ((position) * 4096);

            rear++;
//            getBlockMutex.unlock();

            return valueCache;
        }

        //应该由读磁盘线程来操作！！！
        void addRear(u_int32_t position) {
//            getBlockMutex.lock();
            unique_lock <mutex> lck(mtx[position]);
            remain[position] = 64;
            full[position].notify_all();
//            getBlockMutex.unlock();

        }


        void getNextKeyOffset(u_int16_t &logId, u_int64_t &key, u_int32_t &offset) {

            if (currentSortIndex == sortLogs[currentSortLog]->size()) {
                if (currentSortLog == LOG_NUM - 1)
                    rangeCnt++;
                currentSortLog = (u_int16_t)((currentSortLog + 1) % LOG_NUM);
                currentSortIndex = 0;
            }
            logId = currentSortLog;
            key = sortLogs[currentSortLog]->findKeyByIndex(currentSortIndex);
            offset = sortLogs[currentSortLog]->findValueByIndex(currentSortIndex);

            currentSortIndex++;

            if (rangeCnt == 2)
                logId = LOG_NUM;
        }

    };
}

#endif
//ENGINE_CACHEQUEUECONDITION_H
