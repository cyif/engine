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

//#define VALUE_LOG_SIZE 1024L * 1024 * 4096
//
//#define VALUE_LOG_SIZE 1024 * 4096
#define VALUE_LOG_SIZE 1024L * 256 * 4096
#define KEY_LOG_SIZE 1024L * 256 * 12
#define PER_MAP_SIZE 1024L * 256


#define LOG_NUM 256
#define LOG_NUM_BITS 8


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
            // init
            this->maps = static_cast<LongIntMapForRace **>(malloc(LOG_NUM * sizeof(LongIntMapForRace *)));
            this->keyLogs = static_cast<KeyLog **>(malloc(LOG_NUM * sizeof(KeyLog *)));
            this->valueLogs = static_cast<ValueLog **>(malloc(LOG_NUM * sizeof(ValueLog *)));

            for (int i = 0; i < LOG_NUM; i++) {
                *(maps + i) = new LongIntMapForRace(PER_MAP_SIZE);
                *(keyLogs + i) = new KeyLog(path, i, KEY_LOG_SIZE);
                *(valueLogs + i) = new ValueLog(path, i, VALUE_LOG_SIZE);
            }

            // recover
            std::thread t[LOG_NUM];
            for (int i = 0; i < LOG_NUM; i++) {
                t[i] = std::thread(&PEngine::startAndRecover, this, keyLogs[i], valueLogs[i], maps[i], i);
            }

            for (auto &i : t) {
                i.join();
            }
        }

        ~PEngine() {
            for (int i = 0; i < LOG_NUM; i++) {
                if (keyLogs != NULL){
                    delete keyLogs[i];
                }
                delete valueLogs[i];
                delete maps[i];
            }
            delete[] keyLogs;
            delete[] valueLogs;
            delete[] maps;
        }

        int getLogId(const PolarString &k) {
//            return (*((u_int8_t *) (k.data()))) >> LOG_NUM_BITS;
            return (int) (*((u_int8_t *) (k.data())));
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
            auto k = (long *) key.data();
            auto index = maps[logId]->getOrDefault(*k, -1);
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

        LongIntMapForRace **maps;

        std::mutex logMutex[LOG_NUM];

        void startAndRecover(KeyLog *keyLog, ValueLog *valueLog, LongIntMapForRace *map, const int &id) {
            int pos = 0;
            long k = 0;
            int v = 0;
            int sum = 0;
            while (keyLog->getKey(k, v, pos)) {
                map->put(k, v);
                pos += 12;
                sum ++;
            }
            keyLog->setKeyBufferPosition(pos);
//            valueLog->recover(sum);
            valueLog->setValueFilePosition(((long) sum) << 12);
            valueLog->flush(sum);

            if (sum > 240000){
                delete(keyLog);
            }
        }

    };

}

#endif //ENGINE_RACE_PENGINE_H
