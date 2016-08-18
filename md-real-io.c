// This file is part of MD-REAL-IO.
//
// MD-REAL-IO is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// MD-REAL-IO is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with MD-REAL-IO.  If not, see <http://www.gnu.org/licenses/>.
//
// Author: Julian Kunkel

#include <mpi.h>

#include <stdio.h>

#include "util.h"
#include "option.h"

#ifndef VERSION
  #define VERSION "UNKNOWN"
#endif

char * dir = "./out";
int num = 1250;
int precreate = 2917;
int offset = 24;
int dirs = 10;

int file_size = 3900;

int verbosity = 0;
int thread_report = 0;

int rank;
int size;

void run_benchmark(){

}

int main(int argc, char ** argv){

  MPI_Init(& argc, & argv);
  MPI_Comm_rank(MPI_COMM_WORLD, & rank);
  MPI_Comm_size(MPI_COMM_WORLD, & size);

  option_help options [] = {
    {'T', "target", "Target directory where to run the benchmark.", OPTION_REQUIRED_ARGUMENT, 's', & dir},
    {'N', "num", "Number of I/O operations per thread and directory.", OPTION_OPTIONAL_ARGUMENT, 'd', & num},
    {'P', "precreate", "Number of files to precreate per thread and directory.", OPTION_OPTIONAL_ARGUMENT, 'd', & precreate},
    {'O', "offset", "Offset in ranks between writers and readers. Writers and readers should be located on different nodes.", OPTION_REQUIRED_ARGUMENT, 'd', & offset},
    {'D', "dirs", "Number of I/O operations per thread.", OPTION_OPTIONAL_ARGUMENT, 'd', & dirs},

    {'F', "file-size", "File size for the created files.", OPTION_OPTIONAL_ARGUMENT, 'd', & file_size},

    {0, "thread-reports", "Independent report per thread", OPTION_FLAG, 'd', & thread_report},

    {'v', "verbose", "Increase the verbosity level", OPTION_FLAG, 'd', & verbosity},

    {0, 0, 0, 0, 0, NULL}
    };
  parseOptions(argc, argv, options);

  if (rank == 0){
    printf("MD-REAL-IO (version: %s)\n", VERSION);
  }

  timer bench_start;
  MPI_Barrier(MPI_COMM_WORLD);
  start_timer(& bench_start);

  run_benchmark();
  MPI_Barrier(MPI_COMM_WORLD);

  double runtime = stop_timer(bench_start);
  if (rank == 0){
    printf("Runtime: %.2fs\n", runtime);
  }

  MPI_Finalize();
  return 0;
}
