set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(DK_VERSION 0.3.0)
set(DK_ABI_UPDATE 0.3.0)

if(APPLE)
    set(CMAKE_SHARED_MODULE_CREATE_C_FLAGS
        "${CMAKE_SHARED_MODULE_CREATE_C_FLAGS} -undefined dynamic_lookup")
    cmake_policy(SET CMP0042 NEW)
endif()

option(Build32Bit "Build 32-bit Library" OFF)

# If you need debug, build with:
# cmake -DCMAKE_BUILD_TYPE=Debug ..
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -pedantic -std=c99 -Wno-missing-field-initializers")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-aliasing -Wstrict-overflow")

if(CMAKE_BUILD_TYPE MATCHES Debug)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2")
endif()

add_library(kvidxkit OBJECT
    kvidxkit.c
    kvidxkitAdapterSqlite3.c
    kvidxkitAdapterSqlite3Helper.c
    )

add_library(kvidxkit-shared  MODULE $<TARGET_OBJECTS:kvidxkit>)
add_library(kvidxkit-static  STATIC $<TARGET_OBJECTS:kvidxkit>)
add_library(kvidxkit-library SHARED $<TARGET_OBJECTS:kvidxkit>)

set_target_properties(kvidxkit-shared PROPERTIES PREFIX "") # don't prefix "lib"

set_target_properties(kvidxkit-shared  PROPERTIES OUTPUT_NAME kvidxkit)
set_target_properties(kvidxkit-static  PROPERTIES OUTPUT_NAME kvidxkit)
set_target_properties(kvidxkit-library PROPERTIES OUTPUT_NAME kvidxkit)

target_link_libraries(kvidxkit-library sqlite3-static)
target_link_libraries(kvidxkit-static sqlite3-static)

# SOVERSION only needs to increment when introducing *breaking* changes.
# Otherwise, just increase VERSION with normal feature additions or maint.
set_target_properties(kvidxkit-library PROPERTIES VERSION ${DK_VERSION} SOVERSION ${DK_ABI_UPDATE})

add_executable(kvidxkit-test kvidxkit-test.c)
target_link_libraries(kvidxkit-test kvidxkit-static)

if(APPLE)
    add_custom_command(TARGET kvidxkit-test POST_BUILD COMMAND dsymutil kvidxkit-test COMMENT "Generating OS X Debug Info")
endif()

if(Build32Bit)
    set_target_properties(kvidxkit-shared
                          PROPERTIES COMPILE_FLAGS "-m32" LINK_FLAGS "-m32")
    set_target_properties(kvidxkit-static
                          PROPERTIES COMPILE_FLAGS "-m32" LINK_FLAGS "-m32")
    set_target_properties(kvidxkit-library
                          PROPERTIES COMPILE_FLAGS "-m32" LINK_FLAGS "-m32")
endif()
# vi:ai et sw=4 ts=4:
