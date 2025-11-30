set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(CMAKE_SHARED_MODULE_CREATE_C_FLAGS
        "${CMAKE_SHARED_MODULE_CREATE_C_FLAGS} -undefined dynamic_lookup")
    cmake_policy(SET CMP0042 NEW)
endif()

# LMDB is C99 compatible
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -std=c99")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-parameter")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-format")

if(CMAKE_BUILD_TYPE MATCHES Debug)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g")
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2")
endif()

# LMDB source files - only mdb.c and midl.c are needed for the library
add_library(lmdb OBJECT
    mdb.c
    midl.c
)

add_library(lmdb-static STATIC $<TARGET_OBJECTS:lmdb>)

# LMDB needs pthread on non-Apple Unix systems
if(NOT APPLE AND UNIX)
    target_link_libraries(lmdb-static pthread)
endif()

# vi:ai et sw=4 ts=4:
