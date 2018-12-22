# MD-Workbench [![Build Status](https://travis-ci.org/JulianKunkel/md-workbench.svg?branch=master)](https://travis-ci.org/JulianKunkel/md-workbench)
*****

The MD-Workbench benchmark is an MPI-parallel benchmark to measure metadata (together with small object) performance.
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
* mongodb: depends on MongoDB version 1.5.X and mongoc
  * Ubuntu: Install a recent MongoDB https://docs.mongodb.com/v3.2/tutorial/install-mongodb-on-ubuntu/
   * Update the mongodb driver: http://mongoc.org/libmongoc/1.5.0/installing.html

The test/docker/<SYSTEM> directory contains information how to setup the requirements for various systems.

## Execution

To see the available options run:

                $ ./md-workbench -h

### Example

To run with a single process and defaults parameters that should run quickly just invoke
                $ md-workbench

To set parameters and run with 10 processes:
                $ mpiexec -n 10 ./md-workbench -i=posix -P=10 -D=5 -I=3 -S=3901

The later example runs in three phases, in more detail the following takes place:
   1. Precreate: each process creates 10 files in 5 directories.
   2. Benchmark: each process iterates 3 times: writing a new file, reading and existing file and deleting the file. The offset is a rank offset for the files read and prevents that files are read/deleted by the process that created it.
   3. Cleanup: each process deletes the directories and files it is responsible for.
   A file has the size of 3901 Bytes.

## Testing ##

* Build / unit testing is done using travis: https://travis-ci.org/JulianKunkel/md-workbench
* CTest ist used for testing, just run "make test" or "ctest"
  * Results are pushed to: my.cdash.org/index.php?project=md-workbench by using "ctest -D Experimental"


# Analysis
## Produced output

By default, the benchmark outputs aggregated results for all processes.

The output of two processes and an invocation without arguments is as follows (comments are inline and marked with #):

        #First, information about the run is provided
        Args: ./build/src/md-workbench        
        MD-Workbench total objects: 120000 workingset size: 223.217 MiB (version: 4e8e263@master) time: 2018-03-07 12:13:29
        #Total objects: How many objects in total are created/read/written during the whole run
        #Workingset size: that is the amount of data that is created and used for the benchmark, typically you may want to pick a size that fits or exceeds cache on purpose
        #Version: The git revision of the code
        #Time: The start time when running the benchmark

        #Long version of the options for all internal variables:
        	offset=1
        	interface=posix
        	obj-per-proc=1000
        	latency=
        	precreate-per-set=3000
        	data-sets=10
        	lim-free-mem=0
        	lim-free-mem-phase=0
        	object-size=3901
        	iterations=3
        	waiting-time=0.000000
        	stonewall-timer=0
        	run-info-file=mdtest.status
        	run-precreate
        	run-benchmark
        	run-cleanup

        	root-dir=out

        # Now, the benchmark phases are executed
        # First, the precreation phase that bulk creates files:
        precreate process max:0.6s min:0.6s mean: 0.6s balance:95.0 stddev:0.0 rate:97172.0 iops/s dsets: 20 objects:60000 rate:32.380 dset/s rate:97139.7 obj/s tp:361.4 Mib/s op-max:1.2016e-02s (0 errs) create(8.7990e-06s, 1.4136e-05s, 1.6042e-05s, 1.7927e-05s, 2.0782e-05s, 7.1498e-05s, 1.2016e-02s)

        # Now since we used 3 iterations, the benchmark phase is repeated 3 times
        # We discuss the following (first) result in more detail
        benchmark process max:0.4s min:0.3s mean: 0.3s balance:83.5 stddev:0.0 rate:216259.5 iops/s objects:20000 rate:54064.9 obj/s tp:402.3 Mib/s op-max:2.5220e-03s (0 errs) read(2.7010e-06s, 3.7360e-06s, 4.1480e-06s, 4.4670e-06s, 4.8230e-06s, 6.2970e-06s, 6.4131e-04s) stat(8.7000e-07s, 1.3290e-06s, 1.4510e-06s, 1.5420e-06s, 1.6610e-06s, 2.4170e-06s, 2.5403e-04s) create(9.0040e-06s, 1.3441e-05s, 1.5535e-05s, 1.7213e-05s, 1.8804e-05s, 2.7922e-05s, 2.5220e-03s) delete(6.6800e-06s, 9.5630e-06s, 1.0681e-05s, 1.1479e-05s, 1.2164e-05s, 1.6695e-05s, 7.6339e-04s)
        # The first benchmark result is discussed in detail:
        # max: The maximum runtime for any process
        # min: The minimum runtime for any process
        # mean: arithmethic mean runtime across all processes
        # balance: min runtime divided by max, a balance of 50% means that the minimum process took 50% of the runtime of the longest runing process. A high value is favorable.
        # stddev: the standard deviation of runtime. A low value is favorable.
        # Rate: given in iops/s, each operation like stat, create, read, delete is counted as one IOP, this number is computed based on the global timer
        # objects: total number of objects written/accessessed, since we used two processes, 10 dirs and 1000 obj/dir, the example has 20000 objects
        # tp: the throughput given the file size, an indicative value
        # op-max: the maximum runtime of any I/O operation, i.e., the slowest I/O operation
        # errs:  number of errors, shall be 0 all the time

        # Next comes the statistic for each operation type, the vector is (min, q1, median, q3, q90, q99, max)
        # for example for read(2.7010e-06s, 3.7360e-06s, 4.1480e-06s, 4.4670e-06s, 4.8230e-06s, 6.2970e-06s, 6.4131e-04s)
        # The master process collects all the individual time measurements and compute this value across all processes

        benchmark process max:0.3s min:0.3s mean: 0.3s balance:96.9 stddev:0.0 rate:242381.1 iops/s objects:20000 rate:60595.3 obj/s tp:450.9 Mib/s op-max:8.2705e-04s (0 errs) read(2.6830e-06s, 3.3740e-06s, 4.0300e-06s, 4.3970e-06s, 4.7590e-06s, 6.4030e-06s, 8.2705e-04s) stat(8.6000e-07s, 1.1880e-06s, 1.4350e-06s, 1.5460e-06s, 1.6800e-06s, 2.4580e-06s, 3.8561e-04s) create(8.9650e-06s, 1.1807e-05s, 1.4855e-05s, 1.6555e-05s, 1.7903e-05s, 2.1661e-05s, 4.7924e-04s) delete(6.6300e-06s, 8.4690e-06s, 1.0663e-05s, 1.1824e-05s, 1.2576e-05s, 1.5783e-05s, 5.4768e-04s)
        benchmark process max:0.3s min:0.3s mean: 0.3s balance:95.9 stddev:0.0 rate:234085.1 iops/s objects:20000 rate:58521.3 obj/s tp:435.4 Mib/s op-max:7.8500e-04s (0 errs) read(2.6760e-06s, 3.4720e-06s, 4.0820e-06s, 4.4140e-06s, 4.7550e-06s, 6.0200e-06s, 3.1383e-04s) stat(8.2300e-07s, 1.2210e-06s, 1.4420e-06s, 1.5450e-06s, 1.6650e-06s, 2.2980e-06s, 6.6772e-04s) create(8.9850e-06s, 1.2347e-05s, 1.5173e-05s, 1.6978e-05s, 1.8496e-05s, 2.3212e-05s, 7.8500e-04s) delete(6.9270e-06s, 9.0830e-06s, 1.1119e-05s, 1.2294e-05s, 1.3203e-05s, 1.5773e-05s, 7.0883e-04s)

        # The final cleanup phase which bulk deletes the files
        cleanup process max:0.3s min:0.3s mean: 0.3s balance:98.0 stddev:0.0 rate:174883.3 iops/s objects:60000 dsets: 20 rate:174825.0 obj/s rate:58.275 dset/s op-max:1.9475e-03s (0 errs) delete(5.6650e-06s, 8.8110e-06s, 9.8430e-06s, 1.0481e-05s, 1.1409e-05s, 1.5173e-05s, 1.9475e-03s)

        # The final summary of the run
        Total runtime: 2s time: 2018-03-07 12:13:31
        # runtime: Runtime in seconds
        # time: End timestamp

## Per process information

By default, the benchmark outputs aggregated results for all processes.
Using the flag **--process-reports**, the benchmark outputs for each rank (process) one result line.
An example for the benchmark phase and using two processes looks like:
        benchmark process max:0.4s min:0.4s mean: 0.4s balance:97.7 stddev:0.0 rate:218873.2 ...
        0: benchmark process max:0.4s rate:112010.3 ...
        1: benchmark process max:0.4s rate:109436.6 iops/s objects:10000 rate:27359.1 obj/s ...
Here, the benchmark line gives the aggregated results (as before), while the rank number provides further details.

## Analyzing individual operations

While the benchmark measures the timing for each I/O individually, this information is only output if requested with the **-L** argument:
        $ mpiexec -n 2 ./build/src/md-workbench -L=latency

This writes the measured timings for each I/O and phase to files with the prefix latency, for example, the file **latency-0.00-0-create.csv** is created for the first benchmarking iteration.
The CSV format is simple:

          time,runtime
          0.0000515,3.4478e-05
          ....

Initial support to analyze these files with R are provided in the script in *.R.

By default, only the timings of Rank 0 are stored, to keep all add the flag **--latency-all**.
