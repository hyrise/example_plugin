# consider updating DEPENDENCIES.md when you touch this line
cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(ExamplePlugin)

if(APPLE)
    set(CMAKE_EXE_LINKER_FLAGS -Wl,-export_dynamic)
else()
    set(CMAKE_EXE_LINKER_FLAGS -Wl,--export-dynamic)
endif()

option(ENABLE_UNSUPPORTED_COMPILER "Set to ON to build ExamplePlugin even if the compiler is not supported. Default: OFF" OFF)
function(compiler_not_supported message)
    if (${ENABLE_UNSUPPORTED_COMPILER})
        message(WARNING ${message})
    else()
        message(FATAL_ERROR "${message} You can ignore this error by setting -DENABLE_UNSUPPORTED_COMPILER=ON.")
    endif()
endfunction(compiler_not_supported)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.2)
        compiler_not_supported("Your GCC version ${CMAKE_CXX_COMPILER_VERSION} is too old.")
    endif()
    if (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.9)
        compiler_not_supported("Your GCC version ${CMAKE_CXX_COMPILER_VERSION} is buggy and cannot compile Hyrise (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740). Please use a version before gcc 8.")
    endif()
    if (APPLE)
        # https://software.intel.com/en-us/forums/intel-threading-building-blocks/topic/749200
        compiler_not_supported("We had to drop support for GCC on OS X because it caused segfaults when used with tbb. You can continue, but don't hold us responsible for any segmentation faults.")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
        compiler_not_supported("Your clang version ${CMAKE_CXX_COMPILER_VERSION} is too old.")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    if(NOT EXISTS "/usr/local/opt/llvm/bin/clang")
        set(CMAKE_OSX_INSTALL_TEXT "install the official llvm/clang package using `install.sh` or `brew install llvm --with-toolchain` and then ")
    endif()
    compiler_not_supported("Apple's clang compiler is not supported because it is lacking support for recent C++ features. Please ${CMAKE_OSX_INSTALL_TEXT}run cmake with `-DCMAKE_C_COMPILER=/usr/local/opt/llvm/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++`.")
else()
    compiler_not_supported("You are using an unsupported compiler (${CMAKE_CXX_COMPILER_ID})! Compilation has only been tested with Clang (Linux + OS X) and GCC (Linux).")
endif()

# Enable address and undefined behavior sanitization if requested
option(ENABLE_ADDR_UB_SANITIZATION "Set to ON to build Hyrise with ASAN and UBSAN enabled. Default: OFF" OFF)
if (${ENABLE_ADDR_UB_SANITIZATION})
    # add_compile_options() wants list, CMAKE_EXE_LINKER_FLAGS a string. There are probably cleverer ways than
    # duplicating the flags, but this is probably the simplest solution.
    add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
    set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer")
endif()

# Enable thread sanitization if requested
option(ENABLE_THREAD_SANITIZATION "Set to ON to build Hyrise with TSAN enabled. Default: OFF" OFF)
if (${ENABLE_THREAD_SANITIZATION})
    # add_compile_options() wants list, CMAKE_EXE_LINKER_FLAGS a string. There are probably cleverer ways than
    # duplicating the flags, but this is probably the simplest solution.
    add_compile_options(-fsanitize=thread -O1)
    set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif()

# Set default build type if none was passed on the command line
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

# CMake settings
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH}) # To allow CMake to locate our Find*.cmake files
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}) # Put binaries into root of build tree
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib) # Put libraries into their own dir

set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -march=native")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -march=native")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -march=native")


# Link Time Optimization (LTO)
cmake_policy(SET CMP0069 NEW)
option(ENABLE_LTO "Set to ON to build Hyrise with enabled Link Time Optimization (LTO). Default: OFF" OFF)
if (${ENABLE_LTO})
    if(NOT ${CMAKE_VERSION} VERSION_LESS "3.9.0")
        cmake_minimum_required(VERSION 3.9 FATAL_ERROR)
        include(CheckIPOSupported)
        check_ipo_supported(RESULT ipo_supported OUTPUT ipo_output)
        if(ipo_supported)
            set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
            add_definitions(-DWITH_LTO)
            message(STATUS "Building with Link-Time Optimization")
        else()
            message(WARNING "LTO not supported: ${ipo_output}")
        endif()
    endif()
endif()

# Include sub-CMakeLists.txt
add_subdirectory(hyrise/ EXCLUDE_FROM_ALL)



include_directories(
    ${PROJECT_SOURCE_DIR}/hyrise/src/lib/
)


if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-DIS_DEBUG=1)
else()
    add_definitions(-DIS_DEBUG=0)
endif()

# Global flags and include directories
add_compile_options(-std=c++17 -pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter)

# set(
#     SOURCES
#     example_plugin.cpp
#     example_plugin.hpp
# )

add_library(ExamplePlugin SHARED
    example_plugin.cpp
    example_plugin.hpp
)
add_dependencies(ExamplePlugin hyriseConsole)
target_link_libraries(ExamplePlugin hyrise)
