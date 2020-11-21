cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

include(FetchContent)
FetchContent_Declare(cmake-modules
    GIT_REPOSITORY  https://github.com/abs-tudelft/cmake-modules.git
    GIT_TAG         master
    )
FetchContent_MakeAvailable(cmake-modules)
include(CompileUnits)


project(tpc VERSION 0.0.0 LANGUAGES CXX)

find_package(Arrow 0.15 CONFIG REQUIRED)
find_package(fletcher CONFIG)

if (NOT fletcher)
  add_subdirectory($FLETCHER_DIR/runtime/cpp fletcher)
endif()

add_compile_unit(
    NAME tpc
    TYPE EXECUTABLE
    PRPS
      CXX_STANDARD 11
      CXX_STANDARD_REQUIRED ON
    SRCS
      src/tpc.cpp
    DEPS
      arrow_shared
      fletcher
)

compile_units()
