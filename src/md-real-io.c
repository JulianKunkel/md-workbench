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

struct md_plugin * plugin = NULL;

char * interface = "posix";
int num = 1000;
int precreate = 3000;
int dset_count = 10;

int offset = 1;

int iterations = 1;
int start_index = 0;
int phase_cleanup = 0;
int phase_precreate = 0;
int phase_benchmark = 0;

int file_size = 3900;

int verbosity = 0;
int process_report = 0;
int print_pattern = 0;

int ignore_precreate_errors = 0;
int rank;
int size;

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

void run_precreate(){
  char dset[4096];
  int ret;

  for(int i=0; i < dset_count; i++){
    ret = plugin->def_dset_name(dset, rank, i);
    if (ret != MD_SUCCESS){
      printf("Error defining the dataset name!\n");
      p_dset_creation_errors++;
      continue;
    }
    ret = plugin->create_dset(dset);
    if (ret == MD_NOOP){
      // do not increment any counter
    }else if (ret == MD_SUCCESS){
      p_dsets_created++;
    }else{
      p_dset_creation_errors++;
      if (! ignore_precreate_errors){
        printf("Error while creating the directory %s (%s)\n", dset, strerror(errno));
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
  }

  char * buf = malloc(file_size);
  memset(buf, rank % 256, file_size);

  // create the obj
  for(int d=0; d < dset_count; d++){
    for(int f=0; f < precreate; f++){
      ret = plugin->def_obj_name(dset, rank, d, f);
      if (ret != MD_SUCCESS){
        p_obj_creation_errors++;
        if (! ignore_precreate_errors){
          printf("Error while creating the obj %s\n", dset);
          MPI_Abort(MPI_COMM_WORLD, 1);
        }
      }
      ret = plugin->write_obj(dset, buf, file_size);
      if (ret == MD_NOOP){
        // do not increment any counter
      }else if (ret == MD_SUCCESS){
        p_obj_created++;
      }else{
        p_obj_creation_errors++;
        if (! ignore_precreate_errors){
          printf("Error while creating the obj %s\n", dset);
          MPI_Abort(MPI_COMM_WORLD, 1);
        }
      }
    }
  }
  free(buf);
}


static void print_access_pattern(){
  if (rank == 0){
     printf("I/O pattern\n");
     for(int n=0; n < size; n++){
       for(int d=0; d < dset_count; d++){
         int writeRank = (n + offset * (d+1)) % size;
         int readRank = (n - offset * (d+1)) % size;
         readRank = readRank < 0 ? readRank + size : readRank;
         printf("%d: write: %d read: %d\n", n, writeRank, readRank);
       }
    }
  }
}


/* FIFO: create a new file, write to it. Then read from the first created file, delete it... */
void run_benchmark(int start_index){
  char dset[4096];
  int ret;
  char * buf = malloc(file_size);
  memset(buf, rank % 256, file_size);

  for(int f=0; f < num; f++){
    for(int d=0; d < dset_count; d++){
      int writeRank = (rank + offset * (d+1)) % size;
      const int prevFile = f + start_index;
      ret = plugin->def_obj_name(dset, writeRank, d, precreate + prevFile);
      if (ret != MD_SUCCESS){
        b_obj_creation_errors++;
        continue;
      }

      ret = plugin->write_obj(dset, buf, file_size);
      if (verbosity > 2)
        printf("%d Create %s \n", rank, dset);
      if (ret == MD_ERROR_CREATE){
        if (verbosity)
          printf("Error while creating the obj: %s\n", dset);
        b_obj_creation_errors++;
      }else{
        if (ret == MD_NOOP){
          // do not increment any counter
        }else if (ret != MD_SUCCESS){
          if (verbosity)
            printf("Error while writing the obj: %s\n", dset);
          b_obj_creation_errors++;
        }else{
          b_file_created++;
        }
      }

      int readRank = (rank - offset * (d+1)) % size;
      readRank = readRank < 0 ? readRank + size : readRank;
      ret = plugin->def_obj_name(dset, readRank, d, prevFile);
      ret = plugin->stat_obj(dset, file_size);
      if(ret != MD_SUCCESS && ret != MD_NOOP){
        if (verbosity)
          printf("Error while stating the obj: %s\n", dset);
        b_obj_access_errors++;
        continue;
      }

      if (verbosity > 2){
        printf("%d Access %s \n", rank, dset);
      }
      ret = plugin->read_obj(dset, buf, file_size);
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

      plugin->delete_obj(dset);
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

  for(int d=0; d < dset_count; d++){
    for(int f=0; f < precreate; f++){
      ret = plugin->def_obj_name(dset, rank, d, f+num + start_index);
      ret = plugin->delete_obj(dset);
      if (ret == MD_NOOP){
        // nothing to do
      }else if (ret == MD_SUCCESS){
        c_obj_deleted++;
      }else{
        c_obj_deletion_error++;
      }
    }

    ret = plugin->def_dset_name(dset, rank, d);
    ret = plugin->rm_dset(dset);
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
  b += sprintf(b, "%d\t%.2fs\t\t%.2fs\t\t%.2fs\t", rank, t_precreate_i, t_benchmark_i, t_cleanup_i);
  b += sprintf(b, "     %d(%d)\t%d(%d)", p_dsets_created, p_dset_creation_errors, p_obj_created, p_obj_creation_errors);
  b +=  sprintf(b, "\t%d(%d)\t%d(%d)", b_file_created, b_obj_creation_errors, b_obj_accessed, b_obj_access_errors);
  b += sprintf(b, "\t%d(%d)\n", c_obj_deleted, c_obj_deletion_error);
}

static void prepare_report(){
  int ret;
  double t_max[3] = {t_precreate - t_precreate_i , t_benchmark - t_benchmark_i , t_cleanup - t_cleanup_i};

  uint64_t errors[] = {p_dset_creation_errors, p_obj_creation_errors, b_obj_creation_errors, b_obj_access_errors, c_obj_deletion_error};
  uint64_t correct[] = {p_dsets_created, p_obj_created, b_file_created, b_obj_accessed, c_obj_deleted, c_dsets_deleted};

  if (rank == 0){
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

    double v_pre = LLU correct[1] / (1024.0*1024) * file_size;
    double v_bench = LLU correct[2] / (1024.0*1024) * file_size;
    printf("Volume:              precreate: %.1f MiB benchmark: %.1f MiB\n", v_pre, v_bench);
    if(sumErrors > 0){
      printf("Errors: %llu /Pre dir: %llu obj: %llu /Bench create: %llu access: %llu /Clean obj: %llu\n", LLU sumErrors, LLU errors[0], LLU errors[1], LLU errors[2], LLU errors[3], LLU errors[4] );
    }

    printf("\nCompound performance for the phases:\n");
    if (phase_precreate){
      printf("Precreate: %.1f elements/s (obj+sets) %.1f MiB/s - (create sets, objs and write)\n", (correct[0] + correct[1]) / t_precreate, correct[1] * file_size / t_precreate / (1024.0*1024));
    }
    if (phase_benchmark){
      printf("Benchmark: %.1f iters/s %.1f MiB/s - an iteration is (write new, stat, read and delete old)\n", min(correct[2], correct[3]) / t_benchmark, (correct[2] + correct[3]) * file_size / t_benchmark / (1024.0*1024));
    }
    if (phase_cleanup){
      printf("Cleanup:   %.1f elements/s (sets+objs) - (delete objs and sets)\n", (correct[4]+correct[5]) / t_cleanup );
    }
  }else{ // rank != 0
    ret = MPI_Reduce(t_max, NULL, 3, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)

    ret = MPI_Reduce(errors, NULL, 5, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    ret = MPI_Reduce(correct, NULL, 6, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
  }

  if( process_report ){
    // individual reports per process
    char process_buffer[4096];

    if (rank == 0){
      print_additional_process_report_header();

      print_additional_process_reports(process_buffer);
      printf("%s", process_buffer);
      for(int i=1; i < size; i++){
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
  {'O', "offset", "Offset in ranks between writers and readers. Writers and readers should be located on different nodes.", OPTION_OPTIONAL_ARGUMENT, 'd', & offset},
  {'i', "interface", "The interface (plugin) to use for the test, use list to show all compiled plugins.", OPTION_OPTIONAL_ARGUMENT, 's', & interface},
  {'I', "obj-per-proc", "Number of I/O operations per process and data set.", OPTION_OPTIONAL_ARGUMENT, 'd', & num},
  {'P', "precreate-per-set", "Number of object to precreate per process and data set.", OPTION_OPTIONAL_ARGUMENT, 'd', & precreate},
  {'D', "data-sets", "Number of data sets and communication neighbors per iteration.", OPTION_OPTIONAL_ARGUMENT, 'd', & dset_count},
  {0, "print-pattern", "Print the pattern, the neighbors used in one iteration.", OPTION_FLAG, 'd', & print_pattern},
  {'S', "object-size", "Size for the created objects.", OPTION_OPTIONAL_ARGUMENT, 'd', & file_size},
  {0, "iterations", "Rerun the main phase multiple times", OPTION_OPTIONAL_ARGUMENT, 'd', & iterations},
  {0, "start-index", "Start with this file index in the benchmark; if combined with run-benchmark, this allows to run the benchmark multiple times", OPTION_OPTIONAL_ARGUMENT, 'd', & start_index},
  {0, "run-cleanup", "Run cleanup phase (only run explicit phases)", OPTION_FLAG, 'd', & phase_cleanup},
  {0, "run-precreate", "Run precreate phase", OPTION_FLAG, 'd', & phase_precreate},
  {0, "run-benchmark", "Run benchmark phase", OPTION_FLAG, 'd', & phase_benchmark},
  {0, "ignore-precreate-errors", "Ignore errors occuring during the pre-creation phase", OPTION_FLAG, 'd', & ignore_precreate_errors},
  {0, "process-reports", "Independent report per process", OPTION_FLAG, 'd', & process_report},

  {'v', "verbose", "Increase the verbosity level", OPTION_FLAG, 'd', & verbosity},
  LAST_OPTION
  };

static void find_interface(){
  int is_list = strcmp(interface, "list") == 0 && rank == 0;
  if (is_list){
    printf("Available plugins: ");
  }
  struct md_plugin ** p_it = md_plugin_list;
  while(*p_it != NULL){
    if(is_list){
      printf("%s ", (*p_it)->name);
    }
    if(strcmp((*p_it)->name, interface) == 0) {
      // got it
      plugin = *p_it;
      return;
    }
    p_it++;
  }
  if (rank == 0){
    if(is_list){
      printf("\n");
    }else{
      printf("Could not find plugin for interface: %s\n", interface);
    }
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
}

int main(int argc, char ** argv){
  int ret;
  int printhelp = 0;
  MPI_Init(& argc, & argv);
  MPI_Comm_rank(MPI_COMM_WORLD, & rank);
  MPI_Comm_size(MPI_COMM_WORLD, & size);

  int parsed = parseOptions(argc, argv, options, & printhelp);

  find_interface();

  parseOptions(argc - parsed, argv + parsed, plugin->get_options(), & printhelp);

  if(printhelp != 0){
    if (rank == 0){
      printf("\nSynopsis: %s ", argv[0]);

      print_help(options, 0);

      printf("\nPlugin options for interface %s\n", interface);
      print_help(plugin->get_options(), 1);
    }
    MPI_Finalize();
    if(printhelp == 1){
      exit(0);
    }else{
      exit(1);
    }
  }

  if(print_pattern){
     print_access_pattern();
     MPI_Finalize();
     exit(0);
  }

  if (!(phase_cleanup || phase_precreate || phase_benchmark)){
    // enable all phases
    phase_cleanup = phase_precreate = phase_benchmark = 1;
  }
  if ( start_index > 0 && phase_precreate ){
    printf("The option start_index cannot be used with pre-create!");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  ret = plugin->initialize();
  if (ret != MD_SUCCESS){
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  size_t total_obj_count = dset_count * (size_t) (num + precreate) * size;
  if (rank == 0){
    printf("MD-REAL-IO total objects created: %zu (version: %s)\n", total_obj_count, VERSION);
    if(num > precreate){
      printf("WARNING: num > precreate, this may cause the situation that no objects are available to read\n");
    }
  }

  timer bench_start;
  start_timer(& bench_start);
  timer tmp;

  if (phase_precreate){
    if (rank == 0){
      ret = plugin->prepare_global();
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


  if (phase_benchmark){
    // benchmark phase
    start_timer(& tmp);
    run_benchmark(start_index);
    start_index += num;

    start_index -= num;
    t_benchmark_i = stop_timer(tmp);
    MPI_Barrier(MPI_COMM_WORLD);
    t_benchmark = stop_timer(tmp);
  }

  // cleanup phase
  if (phase_cleanup){
    start_timer(& tmp);
    run_cleanup(start_index);
    t_cleanup_i = stop_timer(tmp);
    MPI_Barrier(MPI_COMM_WORLD);
    t_cleanup = stop_timer(tmp);

    if (rank == 0){
      ret = plugin->purge_global();
      if (ret != MD_SUCCESS && ret != MD_NOOP){
        printf("Rank 0: Error purging the global environment\n");
      }
    }
  }

  t_all = stop_timer(bench_start);

  ret = plugin->finalize();
  if (ret != MD_SUCCESS){
    printf("Error while finalization of module\n");
  }

  prepare_report();

  MPI_Finalize();
  return 0;
}
