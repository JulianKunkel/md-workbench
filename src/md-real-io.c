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

#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include <md_util.h>
#include <md_option.h>

#include <plugins/md-plugin.h>

#include <plugins/md-posix.h>
#include <plugins/md-postgres.h>
#include <plugins/md-mongo.h>
#include <plugins/md-s3.h>
#include <plugins/md-mpi.h>

struct md_plugin * md_plugin_list[] = {
& md_plugin_posix,
#ifdef MD_PLUGIN_MPIIO
& md_plugin_mpi,
#endif
#ifdef MD_PLUGIN_POSTGRES
& md_plugin_postgres,
#endif
#ifdef MD_PLUGIN_MONGO
& md_plugin_mongo,
#endif
#ifdef MD_PLUGIN_S3
& md_plugin_s3,
#endif
NULL
};

#define xstr(s) str(s)
#define str(s) #s

#ifndef VERSION
#ifdef GIT_BRANCH
#define VERSION xstr(GIT_COMMIT_HASH)"@"xstr(GIT_BRANCH)
#else
#define VERSION "UNKNOWN"
#endif
#endif

// statistics for the operations
int p_dsets_created = 0;
int p_dset_creation_errors = 0;
int p_obj_created = 0;
int p_obj_creation_errors = 0;

int c_obj_deleted = 0;
int c_dsets_deleted = 0;
int c_obj_deletion_error = 0;

int b_file_created = 0;
int b_obj_accessed = 0;
int b_obj_creation_errors = 0;
int b_obj_access_errors = 0;

// timers
double t_all, t_precreate = 0, t_benchmark = 0, t_cleanup = 0;
double t_precreate_i = 0, t_benchmark_i = 0, t_cleanup_i = 0;

#define CHECK_MPI_RET(ret) if (ret != MPI_SUCCESS){ printf("Unexpected error in MPI on Line %d\n", __LINE__);}
#define LLU (long long unsigned)
#define min(a,b) (a < b ? a : b)

struct benchmark_options{
  struct md_plugin * plugin;

  char * interface;
  int num;
  int precreate;
  int dset_count;

  int offset;
  int iterations;
  int file_size;

  int start_index;
  int phase_cleanup;
  int phase_precreate;
  int phase_benchmark;

  int verbosity;
  int process_report;
  int print_pattern;

  int ignore_precreate_errors;
  int rank;
  int size;
};

struct benchmark_options o;

void init_options(){
  memset(& o, 0, sizeof(o));
  o.interface = "posix";
  o.num = 1000;
  o.precreate = 3000;
  o.dset_count = 10;
  o.offset = 1;
  o.iterations = 1;
  o.file_size = 3900;
}

void run_precreate(){
  char dset[4096];
  int ret;

  for(int i=0; i < o.dset_count; i++){
    ret = o.plugin->def_dset_name(dset, o.rank, i);
    if (ret != MD_SUCCESS){
      printf("Error defining the dataset name!\n");
      p_dset_creation_errors++;
      continue;
    }
    ret = o.plugin->create_dset(dset);
    if (ret == MD_NOOP){
      // do not increment any counter
    }else if (ret == MD_SUCCESS){
      p_dsets_created++;
    }else{
      p_dset_creation_errors++;
      if (! o.ignore_precreate_errors){
        printf("Error while creating the directory %s (%s)\n", dset, strerror(errno));
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
  }

  char * buf = malloc(o.file_size);
  memset(buf, o.rank % 256, o.file_size);

  // create the obj
  for(int d=0; d < o.dset_count; d++){
    for(int f=0; f < o.precreate; f++){
      ret = o.plugin->def_obj_name(dset, o.rank, d, f);
      if (ret != MD_SUCCESS){
        p_obj_creation_errors++;
        if (! o.ignore_precreate_errors){
          printf("Error while creating the obj %s\n", dset);
          MPI_Abort(MPI_COMM_WORLD, 1);
        }
      }
      ret = o.plugin->write_obj(dset, buf, o.file_size);
      if (ret == MD_NOOP){
        // do not increment any counter
      }else if (ret == MD_SUCCESS){
        p_obj_created++;
      }else{
        p_obj_creation_errors++;
        if (! o.ignore_precreate_errors){
          printf("Error while creating the obj %s\n", dset);
          MPI_Abort(MPI_COMM_WORLD, 1);
        }
      }
    }
  }
  free(buf);
}


static void print_access_pattern(){
  if (o.rank == 0){
     printf("I/O pattern\n");
     for(int n=0; n < o.size; n++){
       for(int d=0; d < o.dset_count; d++){
         int writeRank = (n + o.offset * (d+1)) % o.size;
         int readRank = (n - o.offset * (d+1)) % o.size;
         readRank = readRank < 0 ? readRank + o.size : readRank;
         printf("%d: write: %d read: %d\n", n, writeRank, readRank);
       }
    }
  }
}


/* FIFO: create a new file, write to it. Then read from the first created file, delete it... */
void run_benchmark(int start_index){
  char dset[4096];
  int ret;
  char * buf = malloc(o.file_size);
  memset(buf, o.rank % 256, o.file_size);

  for(int f=0; f < o.num; f++){
    for(int d=0; d < o.dset_count; d++){
      int writeRank = (o.rank + o.offset * (d+1)) % o.size;
      const int prevFile = f + o.start_index;
      ret = o.plugin->def_obj_name(dset, writeRank, d, o.precreate + prevFile);
      if (ret != MD_SUCCESS){
        b_obj_creation_errors++;
        continue;
      }

      ret = o.plugin->write_obj(dset, buf, o.file_size);
      if (o.verbosity > 2)
        printf("%d Create %s \n", o.rank, dset);
      if (ret == MD_ERROR_CREATE){
        if (o.verbosity)
          printf("Error while creating the obj: %s\n", dset);
        b_obj_creation_errors++;
      }else{
        if (ret == MD_NOOP){
          // do not increment any counter
        }else if (ret != MD_SUCCESS){
          if (o.verbosity)
            printf("Error while writing the obj: %s\n", dset);
          b_obj_creation_errors++;
        }else{
          b_file_created++;
        }
      }
      int readRank = (o.rank - o.offset * (d+1)) % o.size;
      readRank = readRank < 0 ? readRank + o.size : readRank;
      ret = o.plugin->def_obj_name(dset, readRank, d, prevFile);
      ret = o.plugin->stat_obj(dset, o.file_size);
      if(ret != MD_SUCCESS && ret != MD_NOOP){
        if (o.verbosity)
          printf("Error while stating the obj: %s\n", dset);
        b_obj_access_errors++;
        continue;
      }

      if (o.verbosity > 2){
        printf("%d Access %s \n", o.rank, dset);
      }
      ret = o.plugin->read_obj(dset, buf, o.file_size);
      if (ret == MD_NOOP){
        // nothing to do
      }else if (ret == MD_ERROR_FIND){
        printf("Error while accessing the file %s (%s)\n", dset, strerror(errno));
        b_obj_access_errors++;
      }else{
        if (ret != MD_SUCCESS){
          printf("Error while reading the file %s (%s)\n", dset, strerror(errno));
          b_obj_access_errors++;
          continue;
        }
        b_obj_accessed++;
      }

      o.plugin->delete_obj(dset);
      if (ret == MD_NOOP){
        // nothing to do
      }else if (ret != MD_SUCCESS){
        b_obj_access_errors++;
      }
    }
  }
  free(buf);
}

void run_cleanup(int start_index){
  char dset[4096];
  int ret;

  for(int d=0; d < o.dset_count; d++){
    for(int f=0; f < o.precreate; f++){
      ret = o.plugin->def_obj_name(dset, o.rank, d, f + o.num + o.start_index);
      ret = o.plugin->delete_obj(dset);
      if (ret == MD_NOOP){
        // nothing to do
      }else if (ret == MD_SUCCESS){
        c_obj_deleted++;
      }else{
        c_obj_deletion_error++;
      }
    }

    ret = o.plugin->def_dset_name(dset, o.rank, d);
    ret = o.plugin->rm_dset(dset);
    if (ret == MD_SUCCESS){
      c_dsets_deleted++;
    }
  }
}

static void print_additional_process_report_header(){
  printf("\nIndividual report:\n");
  printf("Rank\tPrecreate\tBenchmark\tCleanup\t");
  printf("P:CreatedSets\tCreatedObjs ");
  printf("\tB:Created\tAccessed");
  printf("\tC:Deleted\n");
}

static void print_additional_process_reports(char * b){
  b += sprintf(b, "%d\t%.2fs\t\t%.2fs\t\t%.2fs\t", o.rank, t_precreate_i, t_benchmark_i, t_cleanup_i);
  b += sprintf(b, "     %d(%d)\t%d(%d)", p_dsets_created, p_dset_creation_errors, p_obj_created, p_obj_creation_errors);
  b +=  sprintf(b, "\t%d(%d)\t%d(%d)", b_file_created, b_obj_creation_errors, b_obj_accessed, b_obj_access_errors);
  b += sprintf(b, "\t%d(%d)\n", c_obj_deleted, c_obj_deletion_error);
}

static void prepare_report(){
  int ret;
  double t_max[3] = {t_precreate - t_precreate_i , t_benchmark - t_benchmark_i , t_cleanup - t_cleanup_i};

  uint64_t errors[] = {p_dset_creation_errors, p_obj_creation_errors, b_obj_creation_errors, b_obj_access_errors, c_obj_deletion_error};
  uint64_t correct[] = {p_dsets_created, p_obj_created, b_file_created, b_obj_accessed, c_obj_deleted, c_dsets_deleted};

  if (o.rank == 0){
    ret = MPI_Reduce(MPI_IN_PLACE, & t_max, 3, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
    ret = MPI_Reduce(MPI_IN_PLACE, errors, 5, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
    ret = MPI_Reduce(MPI_IN_PLACE, correct, 6, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
    uint64_t sumErrors = errors[0] + errors[1] + errors[2] + errors[3] + errors[4];

    printf("\nTotal runtime:       %.3fs = %.2fs + %.2fs + %.2fs (precreate + benchmark + cleanup)\n", t_all, t_precreate, t_benchmark, t_cleanup);
    printf("Barrier time:        %.2fs; %.2fs; %.2fs\n", t_max[0], t_max[1], t_max[2]);
    printf("Operations:          sets: %llu objs: %llu; write: %llu read: %llu; objs: %llu sets: %llu\n" , LLU correct[0], LLU correct[1], LLU correct[2], LLU correct[3], LLU correct[4], LLU correct[5]);

    double v_pre = LLU correct[1] / (1024.0*1024) * o.file_size;
    double v_bench = LLU correct[2] / (1024.0*1024) * o.file_size;
    printf("Volume:              precreate: %.1f MiB benchmark: %.1f MiB\n", v_pre, v_bench);
    if(sumErrors > 0){
      printf("Errors: %llu /Pre dir: %llu obj: %llu /Bench create: %llu access: %llu /Clean obj: %llu\n", LLU sumErrors, LLU errors[0], LLU errors[1], LLU errors[2], LLU errors[3], LLU errors[4] );
    }

    printf("\nCompound performance for the phases:\n");
    if (o.phase_precreate){
      printf("Precreate: %.1f elements/s (obj+sets) %.1f MiB/s - (create sets, objs and write)\n", (correct[0] + correct[1]) / t_precreate, correct[1] * o.file_size / t_precreate / (1024.0*1024));
    }
    if (o.phase_benchmark){
      printf("Benchmark: %.1f iters/s %.1f MiB/s - an iteration is (write new, stat, read and delete old)\n", min(correct[2], correct[3]) / t_benchmark, (correct[2] + correct[3]) * o.file_size / t_benchmark / (1024.0*1024));
    }
    if (o.phase_cleanup){
      printf("Cleanup:   %.1f elements/s (sets+objs) - (delete objs and sets)\n", (correct[4]+correct[5]) / t_cleanup );
    }
  }else{ // o.rank != 0
    ret = MPI_Reduce(t_max, NULL, 3, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)

    ret = MPI_Reduce(errors, NULL, 5, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    ret = MPI_Reduce(correct, NULL, 6, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
  }

  if( o.process_report ){
    // individual reports per process
    char process_buffer[4096];

    if (o.rank == 0){
      print_additional_process_report_header();

      print_additional_process_reports(process_buffer);
      printf("%s", process_buffer);
      for(int i=1; i < o.size; i++){
        MPI_Recv(process_buffer, 4096, MPI_CHAR, i, 4711, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("%s", process_buffer);
      }
    }else{
      print_additional_process_reports(process_buffer);
      MPI_Send(process_buffer, 4096, MPI_CHAR, 0, 4711, MPI_COMM_WORLD);
    }
  }
}


static option_help options [] = {
  {'O', "offset", "Offset in o.ranks between writers and readers. Writers and readers should be located on different nodes.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.offset},
  {'i', "interface", "The interface (plugin) to use for the test, use list to show all compiled plugins.", OPTION_OPTIONAL_ARGUMENT, 's', & o.interface},
  {'I', "obj-per-proc", "Number of I/O operations per process and data set.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.num},
  {'P', "precreate-per-set", "Number of object to precreate per process and data set.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.precreate},
  {'D', "data-sets", "Number of data sets and communication neighbors per iteration.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.dset_count},
  {0, "print-pattern", "Print the pattern, the neighbors used in one iteration.", OPTION_FLAG, 'd', & o.print_pattern},
  {'S', "object-size", "Size for the created objects.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.file_size},
  {0, "iterations", "Rerun the main phase multiple times", OPTION_OPTIONAL_ARGUMENT, 'd', & o.iterations},
  {0, "start-index", "Start with this file index in the benchmark; if combined with run-benchmark, this allows to run the benchmark multiple times", OPTION_OPTIONAL_ARGUMENT, 'd', & o.start_index},
  {0, "run-cleanup", "Run cleanup phase (only run explicit phases)", OPTION_FLAG, 'd', & o.phase_cleanup},
  {0, "run-precreate", "Run precreate phase", OPTION_FLAG, 'd', & o.phase_precreate},
  {0, "run-benchmark", "Run benchmark phase", OPTION_FLAG, 'd', & o.phase_benchmark},
  {0, "ignore-precreate-errors", "Ignore errors occuring during the pre-creation phase", OPTION_FLAG, 'd', & o.ignore_precreate_errors},
  {0, "process-reports", "Independent report per process", OPTION_FLAG, 'd', & o.process_report},
  {'v', "verbose", "Increase the verbosity level", OPTION_FLAG, 'd', & o.verbosity},
  LAST_OPTION
  };

static void find_interface(){
  int is_list = strcmp(o.interface, "list") == 0 && o.rank == 0;
  if (is_list){
    printf("Available plugins: ");
  }
  struct md_plugin ** p_it = md_plugin_list;
  while(*p_it != NULL){
    if(is_list){
      printf("%s ", (*p_it)->name);
    }
    if(strcmp((*p_it)->name, o.interface) == 0) {
      // got it
      o.plugin = *p_it;
      return;
    }
    p_it++;
  }
  if (o.rank == 0){
    if(is_list){
      printf("\n");
    }else{
      printf("Could not find plugin for interface: %s\n", o.interface);
    }
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
}

int main(int argc, char ** argv){
  int ret;
  int printhelp = 0;
  init_options();
  
  MPI_Init(& argc, & argv);
  MPI_Comm_rank(MPI_COMM_WORLD, & o.rank);
  MPI_Comm_size(MPI_COMM_WORLD, & o.size);
  int parsed = parseOptions(argc, argv, options, & printhelp);

  find_interface();

  parseOptions(argc - parsed, argv + parsed, o.plugin->get_options(), & printhelp);

  if(printhelp != 0){
    if (o.rank == 0){
      printf("\nSynopsis: %s ", argv[0]);

      print_help(options, 0);

      printf("\nPlugin options for interface %s\n", o.interface);
      print_help(o.plugin->get_options(), 1);
    }
    MPI_Finalize();
    if(printhelp == 1){
      exit(0);
    }else{
      exit(1);
    }
  }

  if(o.print_pattern){
     print_access_pattern();
     MPI_Finalize();
     exit(0);
  }

  if (!(o.phase_cleanup || o.phase_precreate || o.phase_benchmark)){
    // enable all phases
    o.phase_cleanup = o.phase_precreate = o.phase_benchmark = 1;
  }
  if ( o.start_index > 0 && o.phase_precreate ){
    printf("The option start_index cannot be used with pre-create!");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  ret = o.plugin->initialize();
  if (ret != MD_SUCCESS){
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  size_t total_obj_count = o.dset_count * (size_t) (o.num + o.precreate) * o.size;
  if (o.rank == 0){
    printf("MD-REAL-IO total objects: %zu (version: %s)\n", total_obj_count, VERSION);
    if(o.num > o.precreate){
      printf("WARNING: num > precreate, this may cause the situation that no objects are available to read\n");
    }
  }

  timer bench_start;
  start_timer(& bench_start);
  timer tmp;

  if (o.phase_precreate){
    if (o.rank == 0){
      ret = o.plugin->prepare_global();
      if ( ret != MD_SUCCESS && ret != MD_NOOP ){
        printf("Rank 0 could not prepare the run, aborting\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    // pre-creation phase
    start_timer(& tmp);
    run_precreate();
    t_precreate_i = stop_timer(tmp);
    MPI_Barrier(MPI_COMM_WORLD);
    t_precreate = stop_timer(tmp);
  }

  int current_index = o.start_index;

  if (o.phase_benchmark){
    // benchmark phase
    start_timer(& tmp);
    for(int i=0; i < o.iterations; i++){

    }
    run_benchmark(current_index);
    current_index += o.num;

    current_index -= o.num;
    t_benchmark_i = stop_timer(tmp);
    MPI_Barrier(MPI_COMM_WORLD);
    t_benchmark = stop_timer(tmp);
  }

  // cleanup phase
  if (o.phase_cleanup){
    start_timer(& tmp);
    run_cleanup(current_index);
    t_cleanup_i = stop_timer(tmp);
    MPI_Barrier(MPI_COMM_WORLD);
    t_cleanup = stop_timer(tmp);

    if (o.rank == 0){
      ret = o.plugin->purge_global();
      if (ret != MD_SUCCESS && ret != MD_NOOP){
        printf("Rank 0: Error purging the global environment\n");
      }
    }
  }

  t_all = stop_timer(bench_start);

  ret = o.plugin->finalize();
  if (ret != MD_SUCCESS){
    printf("Error while finalization of module\n");
  }

  prepare_report();

  MPI_Finalize();
  return 0;
}
