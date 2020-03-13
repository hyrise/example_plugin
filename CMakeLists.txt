# BenchmarkLib disables unity for TPC-DS. Let's try the same here.
set(CMAKE_UNITY_BUILD "OFF")

# Usually, you should not have to touch the following part ##########################################
cmake_minimum_required(VERSION 3.16 FATAL_ERROR)                                                    #
                                                                                                    #
project("Hyrise Example Plugin")                                                                    #
                                                                                                    #
# Include CMakeLists.txt of Hyrise (all the build magic is contained there)                         #
add_subdirectory(hyrise/)                                                                           #
                                                                                                    #
# Add hyrise sources to be able to include headers                                                  #
include_directories(                                                                                #
    ${PROJECT_SOURCE_DIR}/hyrise/src/lib/                                                           #
    ${PROJECT_SOURCE_DIR}/hyrise/src/benchmarklib/                                                  #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/sql-parser/src                                         #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/cxxopts/include                                        #
    ${TBB_INCLUDE_DIR}                                                                              #
    ${Boost_INCLUDE_DIRS}                                                                           #
    ${CMAKE_BINARY_DIR}                                                                             #
)                                                                                                   #
                                                                                                    #
# try to get rid of some TPC-DS unniceties                                                          #
include_directories(                                                                                #
    SYSTEM                                                                                          #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/                                                       #
)                                                                                                   #
                                                                                                    #
add_definitions(-DBOOST_THREAD_VERSION=5)                                                           #
                                                                                                    #
# Global flags and include directories                                                              #
add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter                  #
-Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)                                           #

set(CMAKE_CXX_STANDARD_REQUIRED ON)                                                                 #
set(CMAKE_CXX_STANDARD 20)                                                                          #
                                                                                                    #
#####################################################################################################


### Modify this line to add your own plugin
add_plugin(NAME Driver SRCS driver.cpp driver.hpp plan_cache_csv_exporter.cpp plan_cache_csv_exporter.hpp DEPS hyriseBenchmarkLib hyrisePlayground)
