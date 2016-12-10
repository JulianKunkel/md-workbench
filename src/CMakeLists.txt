add_definitions("-DGIT_COMMIT_HASH=${GIT_COMMIT_HASH}")
add_definitions("-DGIT_BRANCH=${GIT_BRANCH}")

add_executable(md-real-io option.c memory.c md_util.c md-real-io.c ${PLUGINS})
target_link_libraries(md-real-io ${MPI_LIBRARIES} ${MONGOC_LIBRARIES} ${LIBPQ_LIBRARIES} ${LIBS3_LIBRARIES})
include_directories(${MPI_INCLUDE_PATH} ${MONGOC_INCLUDE_DIRS} ${LIBPQ_INCLUDE_DIRS} ${LIBS3_INCLUDE_DIRS})

set_target_properties(md-real-io PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
set_target_properties(md-real-io PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")

install(TARGETS md-real-io RUNTIME DESTINATION bin)

add_test( NAME dummyRun COMMAND mpiexec -n 2 ./md-real-io -i=dummy )
add_test( NAME listModules COMMAND mpiexec -n 2 ./md-real-io -i=list )

# complex tests should not be added here. They can be part of the Debug branch such as:
# add_test( NAME dummyRunDbg CONFIGURATIONS Debug COMMAND mpiexec -n 2 ./md-real-io -i=dummy )
