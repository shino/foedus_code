# Example to read large data set in single transaction

cmake_minimum_required(VERSION 2.4)
cmake_policy(SET CMP0003 NEW)
project(example-big-read-mts)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(FOEDUS_CODE_DIR         ${CMAKE_CURRENT_SOURCE_DIR}/../../)
set(FOEDUS_CORE_INCLUDE_DIR ${FOEDUS_CODE_DIR}/foedus-core/include)
set(FOEDUS_CORE_LIB_DIR     ${FOEDUS_CODE_DIR}/build/foedus-core)

include_directories(${FOEDUS_CORE_INCLUDE_DIR})
link_directories(${FOEDUS_CORE_LIB_DIR})

add_executable(big-read-mts big_read_mts.cpp)
target_link_libraries(big-read-mts foedus-core)

