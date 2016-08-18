# MD-REAL-IO
*****

The MD-REAL-IO benchmark is an MPI-parallel benchmark to measure metadata (together with small file) performance.
It aims to simulate actual user activities on a file system such as compilation.
In contrast to mdtest, it produces access patterns that are not easily cacheable and optimizable by existing (parallel) file systems.
Therefore, it results in much less performance than mdtest (10k IOPS vs. unrealistic 1M IOPS with mdtest).

The pattern is derived from an access pattern from the Parabench benchmark that has been used at DKRZ for acceptance testing.
(see doc/system-load-new-description.pdf for a description of the workload).

## Requirements

The C code needs MPI and is tested with GCC and Intel compilers.
See ./compile.sh how to build the benchmark

## Execution

To see the available options run:

                $ ./md-real-io -h

### Example

Invocation:

                $ mpiexec -n 10 ./md-real-io -T=./test -O=1 -D=5 -P=10 -N=3 -F=3900

This example runs 10 processes. It runs in three phases:

   1. Precreate: each process creates 10 files in 5 directories.
   2. Benchmark: each process iterates 3 times: writing a new file, reading and existing file and deleting the file. The offset is a rank offset for the files read and prevents that files are read/deleted by the process that created it.
   3. Cleanup: each process deletes the directories and files it is responsible for.

A file always has the file size of 3900 Bytes.


