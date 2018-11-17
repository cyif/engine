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

//#define LOG_NUM 256
//#define NUM_PER_SLOT 255000
//#define VALUE_LOG_SIZE NUM_PER_SLOT * 4096
//#define KEY_LOG_SIZE NUM_PER_SLOT * 12
//#define PER_MAP_SIZE NUM_PER_SLOT
//#define RECOVER_THREAD 64


#define LOG_NUM 256
#define NUM_PER_SLOT 251000L
#define VALUE_LOG_SIZE NUM_PER_SLOT * 4096
#define KEY_LOG_SIZE NUM_PER_SLOT * 8
#define PER_MAP_SIZE NUM_PER_SLOT
#define RECOVER_THREAD 64


//#define LOG_NUM 256
//#define NUM_PER_SLOT 1024L
//#define VALUE_LOG_SIZE NUM_PER_SLOT * 4096
//#define KEY_LOG_SIZE NUM_PER_SLOT * 12
//#define PER_MAP_SIZE 1024L
//#define RECOVER_THREAD 64

using namespace std;
using namespace std::chrono;
namespace polar_race {
    static char *prepare() {
        auto buffer = static_cast<char *>(malloc(4096));
        posix_memalign((void **) &buffer, 4096, 4096);
        return buffer;
    }

    static thread_local std::unique_ptr<char> readBuffer(static_cast<char *>(prepare()));

    class PEngine {

    private:
        milliseconds start;
        KeyLog **keyLogs;
        ValueLog **valueLogs;
        SortLog **sortLogs;
        std::mutex logMutex[LOG_NUM];

    public:
        milliseconds now() {
            return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        }

        explicit PEngine(const string &path) {
            // init
            this->sortLogs = static_cast<SortLog **>(malloc(LOG_NUM * sizeof(SortLog *)));
            this->keyLogs = static_cast<KeyLog **>(malloc(LOG_NUM * sizeof(KeyLog *)));
            this->valueLogs = static_cast<ValueLog **>(malloc(LOG_NUM * sizeof(ValueLog *)));

            for (int i = 0; i < LOG_NUM; i++) {
                *(sortLogs + i) = new SortLog(PER_MAP_SIZE);
                *(keyLogs + i) = new KeyLog(path, i, KEY_LOG_SIZE);
                *(valueLogs + i) = new ValueLog(path, i, VALUE_LOG_SIZE);
            }
            this->start = now();
            recover();
            fprintf(stderr, "recover complete. time spent is %lims\n", (now() - start).count());
        }

        ~PEngine() {
            for (int i = 0; i < LOG_NUM; i++) {
                delete keyLogs[i];
                delete valueLogs[i];
                delete sortLogs[i];
            }
            delete[] keyLogs;
            delete[] valueLogs;
            delete[] sortLogs;
            fprintf(stderr, "deleting engine, total life is %lims\n", (now() - start).count());
        }

        void recover(){
            // recover
            std::thread t[RECOVER_THREAD];
            for (int i = 0; i < RECOVER_THREAD; i++) {
                t[i] = std::thread(&PEngine::recoverAndSort, this, i);
            }

            for (auto &i : t) {
                i.join();
            }
        }

        void recoverAndSort(const int& thread_id) {

            char log_per_thread = LOG_NUM / RECOVER_THREAD;
            int id = thread_id * log_per_thread;

            for (int i = 0; i < log_per_thread; i++) {

                KeyLog* keyLog= keyLogs[id];
                ValueLog* valueLog = valueLogs[id];
                SortLog* sortLog = sortLogs[id];

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

        int getLogId(const char* k) {
            return *((u_int8_t *) k);
        }

        void put(const PolarString &key, const PolarString &value) {
            int logId = getLogId(key.data());
            logMutex[logId].lock();
            valueLogs[logId]->putValue(value.data());
            keyLogs[logId]->putValue(key.data());
            logMutex[logId].unlock();
        }

        RetCode read(const PolarString &key, string *value) {
            auto buffer = readBuffer.get();
            int logId = getLogId(key.data());
            auto index = sortLogs[logId]->find(*(u_int64_t*)key.data());

            if (index == -1) {
                return kNotFound;
            } else {
                valueLogs[logId]->readValue(index, buffer);
                value->assign(buffer, 4096);
                return kSucc;
            }
        }

    };

}

#endif //ENGINE_RACE_PENGINE_H
