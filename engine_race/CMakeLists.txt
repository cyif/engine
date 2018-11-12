cmake_minimum_required(VERSION 3.12)
project(engine_race)

set(CMAKE_CXX_STANDARD 11)

include_directories(.)
include_directories(..)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(engine_race
#        benchmark.cc
#        main.cc
        engine_race.cc
        engine_race.h
        RDPEngine.h
        KeyValueLog.h
        sparsepp/spp.h
        sparsepp/spp_config.h
        sparsepp/spp_dlalloc.h
        sparsepp/spp_memory.h
        sparsepp/spp_smartptr.h
        sparsepp/spp_stdint.h
        sparsepp/spp_timer.h
        sparsepp/spp_traits.h
        sparsepp/spp_utils.h
        LongIntMapForRace.h
        CMakeLists.txt
        engine_race.cc
        engine_race.h
#        histogram.cc
#        histogram.h
        KeyValueLog.h
#        likely.h
        LongIntMapForRace.h
        Makefile
#        random.cc
#        random.h
        RDPEngine.h
#        testutil.cc
#        testutil.h
        )
