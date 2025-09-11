cmake_minimum_required(VERSION 3.22)
project(epolluringtest)

set(CMAKE_CXX_STANDARD 23)

add_executable(epolluringtest
        src/main.cpp
#        src/temp.cpp
)

target_include_directories(epolluringtest PRIVATE ${CMAKE_SOURCE_DIR}/include)
