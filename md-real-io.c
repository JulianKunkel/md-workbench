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
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#include "util.h"
#include "option.h"

#ifndef VERSION
  #define VERSION "UNKNOWN"
#endif

char * dir = "./out";
int num = 1000;
int precreate = 3000;
int dirs = 10;

int offset = 24;

int file_size = 3900;

int verbosity = 0;
int thread_report = 0;
int skip_cleanup = 0;

int ignore_precreate_errors = 0;
int rank;
int size;

// statistics for the operations
int p_dirs_created = 0;
int p_dirs_creation_errors = 0;
int p_files_created = 0;
int p_files_creation_errors = 0;

int c_files_deleted = 0;
int c_files_deletion_error = 0;


#define FILENAME_MAX 4096

void run_precreate(){
  char filename[FILENAME_MAX];
  int ret;

  sprintf(filename, "%s/%d", dir, rank);
  ret = mkdir(filename, 0755);
  if (ret == 0){
    p_dirs_created++;
  }else{
    p_dirs_creation_errors++;
    if (! ignore_precreate_errors){
      printf("Error while creating the directory %s (%s)\n", filename, strerror(errno));
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }

  for(int i=0; i < dirs; i++){
    sprintf(filename, "%s/%d/%d", dir, rank, i);
    ret = mkdir(filename, 0755);

    if (ret == 0){
      p_dirs_created++;
    }else{
      p_dirs_creation_errors++;
      if (! ignore_precreate_errors){
        printf("Error while creating the directory %s (%s)\n", filename, strerror(errno));
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
  }

  char * buf = malloc(file_size);
  memset(buf, rank % 256, file_size);

  // create the files
  for(int d=0; d < dirs; d++){
    for(int f=0; f < precreate; f++){
      int fd;
      sprintf(filename, "%s/%d/%d/file-%d", dir, rank, d, f);
      fd = open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
      if (fd == -1) goto error;
      ret = write(fd, buf, file_size);
      if (ret != file_size) goto error;
      close(fd);

      p_files_created++;
      continue;
error:
      p_files_creation_errors++;
      if (! ignore_precreate_errors){
        printf("Error while creating the file %s (%s)\n", filename, strerror(errno));
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
  }
  free(buf);
}

void run_benchmark(){
  char filename[FILENAME_MAX];
  int ret;
}

void run_cleanup(){
  char filename[FILENAME_MAX];
  int ret;

  for(int d=0; d < dirs; d++){
    for(int f=0; f < precreate; f++){
      sprintf(filename, "%s/%d/%d/file-%d", dir, rank, d, f);
      ret = unlink(filename);
      if (ret == 0){
        c_files_deleted++;
      }else{
        c_files_deletion_error++;
      }
    }

    sprintf(filename, "%s/%d/%d", dir, rank, d);
    ret = rmdir(filename);
  }
  sprintf(filename, "%s/%d", dir, rank);
  ret = rmdir(filename);
}

void print_additional_report_header(){
  printf("/Pre CreatedDirs Errors CreateFiles Errors\t/Bench\t/Clean\n");
}

void print_additional_reports(){
  printf("     %d\t\t %d\t%d\t    %d\n", p_dirs_created, p_dirs_creation_errors, p_files_created, p_files_creation_errors);
}

int main(int argc, char ** argv){
  int ret;
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

    {0, "skip-cleanup-phase", "Skip the cleanup phase", OPTION_FLAG, 'd', & skip_cleanup},
    {0, "ignore-precreate-errors", "Ignore errors occuring during the pre-creation phase", OPTION_FLAG, 'd', & ignore_precreate_errors},
    {0, "thread-reports", "Independent report per thread", OPTION_FLAG, 'd', & thread_report},

    {'v', "verbose", "Increase the verbosity level", OPTION_FLAG, 'd', & verbosity},

    {0, 0, 0, 0, 0, NULL}
    };
  parseOptions(argc, argv, options);


  size_t total_files_count = dirs * (size_t) (num + precreate) * size;

  if (rank == 0){
    printf("MD-REAL-IO total files: %zu (version: %s)\n", total_files_count, VERSION);
  }

  timer bench_start;
  double t_all, t_precreate, t_benchmark, t_cleanup;
  // individual timers
  double t_precreate_i, t_benchmark_i, t_cleanup_i = 0;

  double t_precreate_max, t_benchmark_max, t_cleanup_max = 0;

  if (rank == 0){
    ret = mkdir(dir, 0755);
    if ( ret != 0 ){
      printf("Could not create project directory %s (%s)\n", dir, strerror(errno));
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  start_timer(& bench_start);

  timer tmp;

  // pre-creation phase
  start_timer(& tmp);
  run_precreate();
  t_precreate_i = stop_timer(tmp);
  MPI_Barrier(MPI_COMM_WORLD);
  t_precreate = stop_timer(tmp);

  // benchmark phase
  start_timer(& tmp);
  run_benchmark();
  t_benchmark_i = stop_timer(tmp);
  MPI_Barrier(MPI_COMM_WORLD);
  t_benchmark = stop_timer(tmp);

  // cleanup phase
  if (! skip_cleanup){
    start_timer(& tmp);
    run_cleanup();
    t_cleanup_i = stop_timer(tmp);
    MPI_Barrier(MPI_COMM_WORLD);
    t_cleanup = stop_timer(tmp);
  }

  t_all = stop_timer(bench_start);

  t_precreate_max = t_precreate - t_precreate_i;
  t_benchmark_max = t_benchmark - t_benchmark_i;
  t_cleanup_max = t_cleanup - t_cleanup_i;

  if (rank == 0){
    ret = rmdir(dir);
    MPI_Reduce(MPI_IN_PLACE, & t_precreate_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(MPI_IN_PLACE, & t_benchmark_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(MPI_IN_PLACE, & t_cleanup_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    printf("Total runtime: %.2fs precreate: %.2fs benchmark: %.2fs cleanup: %.2fs\n", t_all, t_precreate, t_benchmark, t_cleanup);
    printf("Barrier waiting time after precreate: %.2fs benchmark: %.2fs cleanup: %.2fs\n", t_precreate_max, t_benchmark_max, t_cleanup_max);
  }else{
    MPI_Reduce(& t_precreate_max, NULL, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(& t_benchmark_max, NULL, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(& t_cleanup_max, NULL, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  }

  if( thread_report ){
    // individual reports per thread
    if (rank == 0){
      printf("\nRank\tPrecreate\tBenchmark\tCleanup\t");
      print_additional_report_header();
    }
    for(int i=0; i < size; i++){
      MPI_Barrier(MPI_COMM_WORLD);
      if (rank == i){
        printf("%d\t%.2fs\t\t%.2fs\t\t%.2fs\t", rank, t_precreate_i, t_benchmark_i, t_cleanup_i);
        print_additional_reports();
      }
    }
  }

  MPI_Finalize();
  return 0;
}
