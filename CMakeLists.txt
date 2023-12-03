project(lipp)
cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(MKL_LINK_DIRECTORY "/opt/intel/mkl/lib/intel64")
set(MKL_INCLUDE_DIRECTORY "/opt/intel/mkl/include")

# Set a default build type if none was specified
set(default_build_type "Release")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

add_compile_options(-Wall -fmax-errors=5 -faligned-new -march=native -mtune=native -g)

include_directories(src/core)
link_directories(${MKL_LINK_DIRECTORY})
include_directories(${MKL_INCLUDE_DIRECTORY})

# add_executable(example src/examples/example.cpp)
# add_executable(example_without_fmcd src/examples/example_without_fmcd.cpp)
# add_executable(example_bulk_load src/examples/example_bulk_load.cpp)

# add_executable(range src/range.cpp)

# add_executable(test src/bench/test.cpp)
# target_link_libraries(test PRIVATE mkl_rt -lpthread)

list(APPEND DistList SEQUENTIAL_DIST UNIFORM_DIST LATEST_DIST EXPONENT_DIST ZIPF_DIST HOTSPOT_DIST)

foreach(Dist IN LISTS DistList)
  add_executable(bench_${Dist} src/bench/bench.cpp)
  target_link_libraries(bench_${Dist} PRIVATE mkl_rt -lpthread)
  target_compile_definitions(bench_${Dist} PRIVATE ${Dist} MAX_KEY_SIZE=64)
endforeach()

list(APPEND ycsb_key_size 12 128)
# foreach(key_size IN LISTS ycsb_key_size)
#   add_executable(ycsb_${key_size} src/bench/ycsb.cpp)
#   target_link_libraries(ycsb_${key_size} PRIVATE mkl_rt -lpthread)
#   target_compile_definitions(ycsb_${key_size} PRIVATE MAX_KEY_SIZE=${key_size})
# endforeach()

# list(APPEND twitter_key_size 44 19 47 82)
# foreach(key_size IN LISTS twitter_key_size)
#   add_executable(twitter_${key_size} src/bench/twitter.cpp)
#   target_link_libraries(twitter_${key_size} PRIVATE mkl_rt -lpthread)
#   target_compile_definitions(twitter_${key_size} PRIVATE MAX_KEY_SIZE=${key_size})
# endforeach()