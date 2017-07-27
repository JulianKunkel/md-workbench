cmake_minimum_required (VERSION 3.0)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)

set (PROJECT_NAME MD-REAL-IO)
project (${PROJECT_NAME})

set (VERSION_MAJOR 0)
set (VERSION_MINOR 1)

set(DEV_DIR "${CMAKE_SOURCE_DIR}/dev")
set(DOC_DIR "${CMAKE_SOURCE_DIR}/doc")
set(DEPS_DIR "${CMAKE_SOURCE_DIR}/deps")
set(PLUGIN_DIR "${CMAKE_SOURCE_DIR}/plugins")

include(CTest)
include(FeatureSummary)
include(CheckCSourceCompiles)
include(FindPackageHandleStandardArgs)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

#### Search for dependencies
# set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} /usr/lib64 /usr/local/lib64 /usr/local/lib)

find_package(PkgConfig REQUIRED)
find_package(MPI REQUIRED)

set(CONFIGURE_MINIMAL "FALSE" CACHE BOOL "disable automatic checks for plugin dependencies")

# message(WARNING ${CONFIGURE_MINIMAL})

if (NOT ${CONFIGURE_MINIMAL})
  message(STATUS "Building without extra plugins")
  pkg_search_module(MONGOC libmongoc-1.0>=1.40)
  pkg_check_modules(MONGOC QUIET libmongoc-1.0)

  pkg_search_module(LIBPQ libpq>=9.5)
  pkg_check_modules(LIBPQ QUIET pq)

  pkg_search_module(LIBS3 libs3>=2.0)
  pkg_check_modules(LIBS3 QUIET s3)

  if(NOT LIBS3_VERSION)
    find_library(LIBS3_LIBRARIES NAMES s3 PATHS ENV ADDITIONAL_LIB_PATH)
    find_path(LIBS3_INCLUDE_DIRS NAMES libs3.h PATHS ENV ADDITIONAL_INC_PATH)
    if(LIBS3_INCLUDE_DIRS)
      set(LIBS3_VERSION 1.0)
    else()
      set(LIBS3_INCLUDE_DIRS "")
      set(LIBS3_LIBRARIES "")
      set(LIBS3_LDFLAGS "")
    endif()
  endif()

find_package_handle_standard_args(MONGOC  DEFAULT_MSG  MONGOC_LIBRARIES  MONGOC_INCLUDE_DIRS)
find_package_handle_standard_args(LIBPQ  DEFAULT_MSG  LIBPQ_LIBRARIES  LIBPQ_INCLUDE_DIRS)
find_package_handle_standard_args(LIBS3  DEFAULT_MSG  LIBS3_LIBRARIES  LIBS3_INCLUDE_DIRS)

endif()


include_directories("${CMAKE_SOURCE_DIR}/src" "${CMAKE_SOURCE_DIR}")

## Documentation
find_package(Doxygen)
if(DOXYGEN_FOUND)
    add_custom_target(doc
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM
    )
    configure_file(${DOC_DIR}/Doxyfile.in D/home/julian/Dokumente/DKRZ/wr-git/md-real-io/plugin/md-posix.coxyfile @ONLY)
endif(DOXYGEN_FOUND)
##### END Documentation


# compile flags
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu99 -pedantic -Wall -Wextra -pedantic-errors -Wno-unused-parameter -Werror")
set (CMAKE_C_FLAGS_DEBUG   "-O0 -g3")
set (CMAKE_C_FLAGS_RELEASE "-O3")


## identify which plugins are compilable
set(PLUGINS "${PLUGIN_DIR}/md-dummy.c" "${PLUGIN_DIR}/md-posix.c" "${PLUGIN_DIR}/md-mpi.c")
add_definitions("-DMD_PLUGIN_MPIIO")

# GIT VERSIONs:
execute_process(COMMAND git rev-parse --abbrev-ref HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} OUTPUT_VARIABLE GIT_BRANCH OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(COMMAND git log -1 --format=%h
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}  OUTPUT_VARIABLE GIT_COMMIT_HASH  OUTPUT_STRIP_TRAILING_WHITESPACE )

if(MONGOC_FOUND)
  set(PLUGINS "${PLUGIN_DIR}/md-mongo.c" ${PLUGINS})
  add_definitions("-DMD_PLUGIN_MONGO")
endif()
if(LIBPQ_FOUND)
  set(PLUGINS "${PLUGIN_DIR}/md-postgres.c" ${PLUGINS})
  add_definitions("-DMD_PLUGIN_POSTGRES")
endif()
if(LIBS3_FOUND)
  set(PLUGINS "${PLUGIN_DIR}/md-s3.c" ${PLUGINS})
  add_definitions("-DMD_PLUGIN_S3")
endif()

set(PLUGINS ${PLUGINS} CACHE FILEPATH "enabled plugins")

SUBDIRS (src)

feature_summary(WHAT ALL)
