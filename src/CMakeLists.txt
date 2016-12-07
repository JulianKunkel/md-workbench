add_definitions("-DGIT_COMMIT_HASH=${GIT_COMMIT_HASH}")
add_definitions("-DGIT_BRANCH=${GIT_BRANCH}")

add_executable(md-real-io option.c md-real-io.c ${PLUGINS})
target_link_libraries(md-real-io ${MPI_LIBRARIES} ${MONGOC_LIBRARIES} ${LIBPQ_LIBRARIES} ${LIBS3_LIBRARIES})
include_directories(${MPI_INCLUDE_PATH} ${MONGOC_INCLUDE_DIRS} ${LIBPQ_INCLUDE_DIRS} ${LIBS3_INCLUDE_DIRS})

set_target_properties(md-real-io PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
set_target_properties(md-real-io PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")

install(TARGETS md-real-io RUNTIME DESTINATION bin)
