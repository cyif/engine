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

#define CACHE_SIZE 80000
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
        std::mutex canReadMutex;


        SortLog **sortLogs;
        u_int16_t currentSortLog;
        int currentSortIndex;
        int rangeCnt = 0;

        std::mutex mtx[CACHE_SIZE];
        std::condition_variable empty[CACHE_SIZE];
        std::condition_variable full[CACHE_SIZE];
        int remain[CACHE_SIZE]{0};
        int real[CACHE_SIZE]{0};



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
            while (remain[position] == 0 || readPositions[threadId] != real[position])
                full[position].wait(lck);
//                full[position].wait_for(lck, chrono::milliseconds(10));

            lck.unlock();

            key = PolarString((char *)(&keys[position]), 8);
            value = PolarString(values + (position * 4096), 4096);
            readPositions[threadId]++;

            lck.lock();
            remain[position]--;
            if (remain[position] == 0) {
//                printf("empty position %d\n",position);
                empty[position].notify_all();
            }
            lck.unlock();

        }

        // 读磁盘线程获取下一个写缓存块，以及文件位置，logId == LOG_NUM 说明读取结束
        char* getPutBlock(u_int16_t &logId, u_int32_t &offset, u_int32_t & realNum) {

            getBlockMutex.lock();
            int position = getRealQueuePosition(rear);
            u_int64_t key;
            getNextKeyOffset(logId, key, offset);
            char* valueCache = values + ((position) * 4096);

            realNum = rear;
            rear++;
            getBlockMutex.unlock();

            unique_lock <mutex> lck(mtx[position]);
            while (remain[position] > 0) {
                empty[position].wait(lck);
            }

            keys[position] = key;

            return valueCache;

            //            printf("\n========\n");
//            printf("remain: %lu\n", remain[position]);
//            printf("logId: %lu\n", logId);
////            printf("key: %lu\n", swapEndian(keys[position]));
//            printf("offset: %lu\n", offset);
        }


        //应该由读磁盘线程来操作！！！
        void addRear(u_int32_t realNum) {
            int position = getRealQueuePosition(realNum);
            unique_lock <mutex> lck(mtx[position]);
//            printf("addRear remain: %lu\n", remain[position]);
            remain[position] = 64;
            real[position] = realNum;
            full[position].notify_all();
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
