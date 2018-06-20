cmake_minimum_required(VERSION 3.9.1)
project(influxdb-hp)

set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if (WIN32)
    set(EVENT__DISABLE_OPENSSL ON CACHE BOOL "EVENT__DISABLE_OPENSSL" FORCE)
	if(MINGW)
		add_definitions(-DEVENT__HAVE_STRTOK_R)
	endif()
    add_subdirectory(libevent)
    include_directories(libevent/include libevent/WIN32-Code/nmake)
endif ()

set(RAPIDJSON_BUILD_EXAMPLES OFF CACHE BOOL "RAPIDJSON_BUILD_EXAMPLES" FORCE)
set(RAPIDJSON_BUILD_TESTS OFF CACHE BOOL "RAPIDJSON_BUILD_EXAMPLES" FORCE)
add_subdirectory(rapidjson)

add_library(influxdb include/client.h src/client.cpp src/series.cpp src/util.h src/json-readers.h include/fetch.h src/cache.h farmhash/src/farmhash.cc cpp-base64/base64.cpp)
add_library(influxdb_shared SHARED src/client.cpp  src/series.cpp)

set(EVPP_VCPKG_BUILD ON CACHE BOOL "EVPP_VCPKG_BUILD" FORCE)
add_subdirectory(evpp)

target_link_libraries(influxdb evpp_static)
if(WIN32 AND NOT MINGW)
	target_link_libraries(influxdb_shared evpp_static $<$<BOOL:${WIN32}>:event_static> )
else()
	target_link_libraries(influxdb_shared evpp)
endif()

if (WIN32)
    target_link_libraries(influxdb ws2_32)
    target_link_libraries(influxdb_shared ws2_32)
endif ()

include_directories(include evpp rapidjson/include date/include)



#############################################
# Tests
include(GoogleTest)
add_executable(influx_test
        test/fetch.cpp
        )
gtest_add_tests(influx_test "" AUTO)
include_directories(googletest/googletest/include)
add_subdirectory(googletest)
target_link_libraries(influx_test gtest_main influxdb_shared)
#target_compile_options(cmtctest PRIVATE -Wa,-mbig-obj)



