cmake_minimum_required(VERSION 3.12)
project(engine_race)

set(CMAKE_CXX_STANDARD 11)

include_directories(.)
include_directories(..)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(engine_race
        test/benchmark.cc
#        main.cc
        engine_race/engine_race.cc
        engine_race/engine_race.h
        engine_race/PEngine.h
        engine_race/key_log.h
        engine_race/value_log.h
        engine_race/LongIntMapForRace.h
        CMakeLists.txt
        test/histogram.cc
        test/histogram.h
        test/likely.h
        test/random.cc
        test/random.h
        test/testutil.cc
        test/testutil.h
        )
