//
// Created by 杨煜溟 on 2018/11/20.
//

#ifndef ENGINE_CACHEQUEUE_H
#define ENGINE_CACHEQUEUE_H

#include "SortLog.h"
#include "../include/polar_string.h"

#include <mutex>


#define CACHE_SIZE 1024
#define LOG_NUM 256

using namespace std;

namespace polar_race {
    class CacheQueue {
    private:
        u_int32_t front;
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

    public:
        explicit CacheQueue(SortLog **sortLogs) : sortLogs(sortLogs), front(0), rear(0), currentSortLog(0),
                                                  currentSortIndex(0) {
            this->keys = (u_int64_t *) (malloc((CACHE_SIZE) * sizeof(u_int64_t)));
            this->values = (char *) (malloc((CACHE_SIZE) * 4096));
            posix_memalign((void **) &values, 4096, CACHE_SIZE * 4096);
        }

        ~CacheQueue(){
            delete keys;
            delete values;
        }
        u_int32_t getRealQueuePosition(u_int32_t &position) {
            return position % CACHE_SIZE;
        }

        //64个线程读取下一个缓存块，返回key和cache地址
        void read(int &threadId, PolarString & key, PolarString & value) {
            while (readPositions[threadId] == rear) {
            }

            u_int32_t position = getRealQueuePosition(readPositions[threadId]);

            key = PolarString((char *)(&keys[position]), 8);
            value = PolarString(values + (position * 4096), 4096);

            readPositions[threadId]++;
        }

        // 读磁盘线程获取下一个写缓存块，以及文件位置，logId == LOG_NUM 说明读取结束
        char* getPutBlock(u_int16_t &logId, u_int32_t &offset) {
//            getBlockMutex.lock();

            //尾指针rear+1，如果追上头指针front, 根据readPositions更新front
            while ((rear + 1) % CACHE_SIZE == front % CACHE_SIZE)
                updateFront();

            getNextKeyOffset(logId, keys[rear % CACHE_SIZE], offset);
            char *valueCache = values + ((rear % CACHE_SIZE) * 4096);

            return valueCache;

//            getBlockMutex.unlock();
        }

        //应该由读磁盘线程来操作！！！
        void addRear() {
            rear++;
        }

        bool updateFront() {
            u_int32_t lastReadPosition = UINT32_MAX;
            for (u_int32_t readPosition : readPositions)
                if (readPosition < lastReadPosition)
                    lastReadPosition = readPosition;

            if (lastReadPosition == UINT32_MAX)
                return false;
            else {
                front = lastReadPosition;
                return true;
            }
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

#endif //ENGINE_CACHEQUEUE_H
