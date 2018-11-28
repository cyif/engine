//
// Created by parallels on 11/11/18.
//

#ifndef ENGINE_KEY_LOG_H
#define ENGINE_KEY_LOG_H

#include <string>

using namespace std;

namespace polar_race {

    class KeyLog {
    private:
        long keyBufferPosition;
        u_int8_t * keyBuffer;

    public:

        explicit KeyLog(u_int8_t *keyBuffer) : keyBuffer(keyBuffer), keyBufferPosition(0) {
        }

        inline void putValue(const char * key) {
            *((u_int64_t *) (keyBuffer + keyBufferPosition)) = *((u_int64_t *) key);
            keyBufferPosition += 8;
        }

        inline bool getKey(u_int64_t & key) {
            key = *(u_int64_t*)(keyBuffer + keyBufferPosition);
            keyBufferPosition += 8;
            return key != 0;
        }

        void setKeyBufferPosition(long position) {
            this->keyBufferPosition = position;
        }

    };
}
#endif //ENGINE_KEY_LOG_H
