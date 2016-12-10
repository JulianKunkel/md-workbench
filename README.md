# MD-REAL-IO [![Build Status](https://travis-ci.org/JulianKunkel/md-real-io.svg?branch=master)](https://travis-ci.org/JulianKunkel/md-real-io)
*****

The MD-REAL-IO benchmark is an MPI-parallel benchmark to measure metadata (together with small object) performance.
It aims to simulate actual user activities on a file system such as compilation.
In contrast to mdtest, it produces access patterns that are not easily cacheable and optimizable by existing (parallel) file systems.
Therefore, it results in much less performance than mdtest (10k IOPS vs. 100k IOPS with mdtest).

The pattern is derived from an access pattern from the Parabench benchmark that has been used at DKRZ for acceptance testing.
(see doc/system-load-new-description.pdf for a description of the workload).


## Requirements

The C code needs MPI and is tested with GCC and Intel compilers.
See ./compile.sh how to build the benchmark

Supported plugins add further requirements:
* postgres: needs the libpq
  * Ubuntu16.04: libpq-dev
* mongodb: depends on the mongoc and bson
  * Ubuntu: Install a recent MongoDB https://docs.mongodb.com/v3.2/tutorial/install-mongodb-on-ubuntu/
   * Update the mongodb driver: http://mongoc.org/libmongoc/1.3.0/installing.html

## Execution

To see the available options run:

                $ ./md-real-io -h

### Example

Invocation:

                $ mpiexec -n 10 ./md-real-io -i=posix -P=10 -D=5 -I=3 -S=3900 

This example runs 10 processes. It runs in three phases:

   1. Precreate: each process creates 10 files in 5 directories.
   2. Benchmark: each process iterates 3 times: writing a new file, reading and existing file and deleting the file. The offset is a rank offset for the files read and prevents that files are read/deleted by the process that created it.
   3. Cleanup: each process deletes the directories and files it is responsible for.
   A file has the size of 3900 Bytes.

## Testing ##

* Build / unit testing is done using travis: https://travis-ci.org/JulianKunkel/md-real-io
* CTest ist used for testing, just run "make test" or "ctest"
  * Results are pushed to: my.cdash.org/index.php?project=md-real-io by using "ctest -D Experimental"
