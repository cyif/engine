//
// Created by Wayne Wang on 2018/10/30.
//

#ifndef ENGINE_RACE_RDPENGINE_H
#define ENGINE_RACE_RDPENGINE_H


#include <iostream>
#include <sys/mman.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>

#include <unistd.h>
#include <string>
#include <mutex>
#include <include/engine.h>
#include <thread>
#include <atomic>
#include "include/polar_string.h"
#include "KeyValueLog.h"
#include "chrono"

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


    class RDPEngine {

    public:
        RDPEngine(const string &path) {
            this->start = now();
            this->keyValueLogs = static_cast<KeyValueLog **>(malloc(64 * sizeof(KeyValueLog *)));
            std::thread t[64];
            for (int i = 0; i < 64; ++i) {
                auto p = keyValueLogs + i;
                t[i] = std::thread(initialKeyValueLog, p, path, i);
            }

            for (auto &i : t) {
                i.join();
            }
            fprintf(stderr, "Open database complete. time spent is %lims\n", (now() - start).count());
        }

        ~RDPEngine() {
            for (int i = 0; i < 64; i++)
                delete keyValueLogs[i];
            delete[]keyValueLogs;
            fprintf(stderr, "deleting rdp engine, total life is %lims\n", (now() - start).count());
        }

        KeyValueLog *selectKVLog(const PolarString &k) {
            auto id = (*((u_int8_t *) (k.data()))) % 64;
            return keyValueLogs[id];
        }

        void put(const PolarString &key, const PolarString &value) {
            auto keyValueLog = selectKVLog(key);
            keyValueLog->putValue(key, value.data());
        }

        RetCode read(const PolarString &key, string *value) {
            auto keyValueLog = selectKVLog(key);
            auto buffer = readBuffer.get();
            auto code = keyValueLog->readValue(key, buffer);
            value->assign(buffer, 4096);
            return code;
        }

    private:
        milliseconds start;
        KeyValueLog **keyValueLogs;

        static void initialKeyValueLog(KeyValueLog **pKeyValueLog, const string &path, int id) {
            *pKeyValueLog = new KeyValueLog(path, id);
        }
    };

}

#endif //ENGINE_RACE_RDPENGINE_H
