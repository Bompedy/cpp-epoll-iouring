cmake_minimum_required(VERSION 3.22)
project(epolltest)

set(CMAKE_CXX_STANDARD 23)

add_executable(epolltest main.cpp)


target_include_directories(epolltest PRIVATE ${CMAKE_SOURCE_DIR}/include)
