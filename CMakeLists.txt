cmake_minimum_required(VERSION 3.1)
project(influxdb-hp)

set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if (WIN32)
    set(EVENT__DISABLE_OPENSSL ON CACHE BOOL "EVENT__DISABLE_OPENSSL" FORCE)
    add_definitions(-DEVENT__HAVE_STRTOK_R)
    add_subdirectory(libevent)
    include_directories(libevent/include libevent/WIN32-Code/nmake)
endif ()

set(RAPIDJSON_BUILD_EXAMPLES OFF CACHE BOOL "RAPIDJSON_BUILD_EXAMPLES" FORCE)
set(RAPIDJSON_BUILD_TESTS OFF CACHE BOOL "RAPIDJSON_BUILD_EXAMPLES" FORCE)
add_subdirectory(rapidjson)

add_library(influxdb include/client.h src/client.cpp src/util.h src/json-readers.h)
add_library(influxdb_shared SHARED src/client.cpp)

#add_compile_options(-Wno-builtin-macro-redefined -Wp,-w)

set(EVPP_VCPKG_BUILD ON CACHE BOOL "EVPP_VCPKG_BUILD" FORCE)
add_subdirectory(evpp)

target_link_libraries(influxdb evpp_static)
target_link_libraries(influxdb_shared evpp)


if (WIN32)
    target_link_libraries(influxdb ws2_32)
    target_link_libraries(influxdb_shared ws2_32)
endif ()

include_directories(mongoose include evpp rapidjson/include date/include)
