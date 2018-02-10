cmake_minimum_required (VERSION 3.1)
project (influxdb-hp)


#set(EVENT__DISABLE_OPENSSL ON CACHE BOOL "EVENT__DISABLE_OPENSSL" FORCE)
add_subdirectory(libevent)

add_subdirectory(rapidjson)


add_library(influxdb-hp include/client.h src/client.cpp src/util.h src/json-readers.h)

add_compile_options(-Wno-builtin-macro-redefined -Wp,-w)

add_subdirectory(evpp)
target_link_libraries(influxdb-hp evpp_static ws2_32)

include_directories(mongoose include evpp libevent/include libevent/WIN32-Code/nmake rapidjson/include date/include)