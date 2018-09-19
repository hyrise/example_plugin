# Usually, you should not have to touch the following part ##########################################
cmake_minimum_required(VERSION 3.5 FATAL_ERROR)                                                     #
                                                                                                    #
# Include CMakeLists.txt of Hyrise (all the build magic is contained there)                         #
add_subdirectory(hyrise/ EXCLUDE_FROM_ALL)                                                          #
                                                                                                    #
# Add hyrise sources to be able to include headers                                                  #
include_directories(                                                                                #
    ${PROJECT_SOURCE_DIR}/hyrise/src/lib/                                                           #
)                                                                                                   #
                                                                                                    #
# This will be used by the DebugAssert macro to output                                              #
# a file path relative to CMAKE_SOURCE_DIR                                                          #
string(LENGTH "${CMAKE_SOURCE_DIR}/" SOURCE_PATH_SIZE)                                              #
if (CMAKE_CXX_COMPILER_LAUNCHER)                                                                    #
    if (${CMAKE_CXX_COMPILER_LAUNCHER} STREQUAL "ccache")                                           #
        string(LENGTH "" SOURCE_PATH_SIZE)                                                          #
    endif()                                                                                         #
endif()                                                                                             #
add_definitions("-DSOURCE_PATH_SIZE=${SOURCE_PATH_SIZE}")                                           #
                                                                                                    #
# Global flags and include directories                                                              #
add_compile_options(-std=c++17 -pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter)      #
                                                                                                    #
#####################################################################################################


### Modify this line to add your own plugin
add_plugin(NAME ExamplePlugin SRCS example_plugin.cpp example_plugin.hpp DEPS hyriseConsole)
