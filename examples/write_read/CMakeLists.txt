# Write & read example

## Assumed:
## 1. libfoedus-core has already been built under foedus_code/build/ dir
## 2. this project directory is placed in foedus_code/examples/first,
## Otherwise, changing paths FOEDUS_CORE_INCLUDE_DIR and FOEDUS_CORE_LIB_DIR should work.

cmake_minimum_required(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)
project(example-write-read)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(FOEDUS_CODE_DIR         ${CMAKE_CURRENT_SOURCE_DIR}/../../)
set(FOEDUS_CORE_INCLUDE_DIR ${FOEDUS_CODE_DIR}/foedus-core/include)
set(FOEDUS_CORE_LIB_DIR     ${FOEDUS_CODE_DIR}/build/foedus-core)

include_directories(${FOEDUS_CORE_INCLUDE_DIR})
link_directories(${FOEDUS_CORE_LIB_DIR})

add_executable(write_read write_read.cpp)
target_link_libraries(write_read foedus-core)

