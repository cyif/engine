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
#include "../include/polar_string.h"
#include "../include/engine.h"
#include "key_log.h"
#include "value_log.h"
#include "SortLog.h"

#define LOG_NUM 256
#define NUM_PER_SLOT 1024L * 256
#define VALUE_LOG_SIZE NUM_PER_SLOT * 4096
#define KEY_LOG_SIZE NUM_PER_SLOT * 12
#define PER_MAP_SIZE NUM_PER_SLOT


using namespace std;
namespace polar_race {
    static char *prepare() {
        auto buffer = static_cast<char *>(malloc(4096));
        posix_memalign((void **) &buffer, 4096, 4096);
        return buffer;
    }

    static thread_local std::unique_ptr<char> readBuffer(static_cast<char *>(prepare()));

    class PEngine {

    public:
        explicit PEngine(const string &path) {

            this->sortLogs = static_cast<SortLog **>(malloc(LOG_NUM * sizeof(SortLog *)));
            this->keyLogs = static_cast<KeyLog **>(malloc(LOG_NUM * sizeof(KeyLog *)));
            this->valueLogs = static_cast<ValueLog **>(malloc(LOG_NUM * sizeof(ValueLog *)));

            for (int i = 0; i < LOG_NUM; i++) {
                *(sortLogs + i) = new SortLog(PER_MAP_SIZE);
                *(keyLogs + i) = new KeyLog(path, i, KEY_LOG_SIZE);
                *(valueLogs + i) = new ValueLog(path, i, VALUE_LOG_SIZE);
            }

            recover();
        }

        ~PEngine() {
            for (int i = 0; i < LOG_NUM; i++) {
                delete keyLogs[i];
                delete valueLogs[i];
//                delete maps[i];
                delete sortLogs[i];
            }
            delete[] keyLogs;
            delete[] valueLogs;
//            delete[] maps;
            delete[] sortLogs;

//            std::cout << "close" << endl;

        }

        void recover(){
            // recover
            std::thread t[LOG_NUM];
            for (int i = 0; i < LOG_NUM; i++) {
                t[i] = std::thread(&PEngine::recoverAndSort, this, i);
            }

            for (auto &i : t) {
                i.join();
            }
        }

        void recoverAndSort(const int &id) {
            KeyLog* keyLog= keyLogs[id];
            ValueLog* valueLog = valueLogs[id];
            SortLog* sortLog = sortLogs[id];

            int pos = 0;
            u_int64_t k = 0;
            int v = 0;
            int sum = 0;

            while (keyLog->getKey(k, v, pos)) {
                sortLog->put(k, v);
                pos += 12;
                sum ++;
            }

            sortLog->quicksort();

            keyLog->setKeyBufferPosition(pos);
            valueLog->setValueFilePosition(((long) sum) << 12);
            valueLog->flush(sum);


        }


        int getLogId(const PolarString &k) {
            return (*((u_int8_t *) (k.data())));
        }

        void put(const PolarString &key, const PolarString &value) {

            auto logId = getLogId(key);
            logMutex[logId].lock();
            int index = valueLogs[logId]->putValue(value.data());
            keyLogs[logId]->putValue(key, index);
            logMutex[logId].unlock();
        }

        RetCode read(const PolarString &key, string *value) {
            auto buffer = readBuffer.get();
            auto logId = getLogId(key);
            auto k = (u_int64_t *) key.data();
            auto index = sortLogs[logId]->find(*k);

            if (index == -1) {
                return kNotFound;
            } else {
                valueLogs[logId]->readValue(index, buffer);
                value->assign(buffer, 4096);
                return kSucc;
            }
        }



    private:
        KeyLog **keyLogs;

        ValueLog **valueLogs;

        SortLog **sortLogs;

        std::mutex logMutex[LOG_NUM];
    };

}

#endif //ENGINE_RACE_PENGINE_H
