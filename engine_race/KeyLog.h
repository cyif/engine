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
        int keyBufferPosition;
        u_int64_t * keyBuffer;

    public:

        explicit KeyLog(u_int64_t *keyBuffer) : keyBuffer(keyBuffer), keyBufferPosition(0) {
        }

        inline void putKey(const char * key) {
            *(keyBuffer + keyBufferPosition) = *((u_int64_t *) key);
            keyBufferPosition++;
        }

        inline bool getKey(u_int64_t & key) {
            key = *(keyBuffer + keyBufferPosition);
            keyBufferPosition++;
            return key != 0;
        }

        void setKeyBufferPosition(int position) {
            this->keyBufferPosition = position;
        }

    };
}
#endif //ENGINE_KEY_LOG_H
