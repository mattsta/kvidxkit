cmake_minimum_required(VERSION 4.1)
project(lmdb C)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_DISABLE_SOURCE_CHANGES  ON)

if (TARGET lmdb)
    return()
endif(TARGET lmdb)

add_subdirectory(libraries/liblmdb)
# vi:ai et sw=4 ts=4:
