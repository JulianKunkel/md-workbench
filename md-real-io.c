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

struct md_plugin * md_plugin_list[] = {
#ifdef MD_PLUGIN_POSIX
& md_plugin_posix,
#endif
NULL
};


#ifndef VERSION
  #define VERSION "UNKNOWN"
#endif

struct md_plugin * plugin = NULL;

char * dir = "./out";
char * interface = "posix";
int num = 1000;
int precreate = 3000;
int dirs = 10;

int offset = 1;

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
int c_dirs_deleted = 0;
int c_files_deletion_error = 0;

int b_file_created = 0;
int b_file_accessed = 0;
int b_file_creation_errors = 0;
int b_file_access_errors = 0;

// timers
double t_all, t_precreate, t_benchmark, t_cleanup = 0;
double t_precreate_i, t_benchmark_i, t_cleanup_i = 0;

#ifndef FILENAME_MAX
#define FILENAME_MAX 4096
#endif

#define CHECK_MPI_RET(ret) if (ret != MPI_SUCCESS){ printf("Unexpected error in MPI on Line %d\n", __LINE__);}
#define LLU (long long unsigned)
#define min(a,b) (a < b ? a : b)

void run_precreate(){
  char filename[FILENAME_MAX];

  int ret;

  ret = plugin->create_rank_dir(filename, dir, rank);
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
    ret = plugin->create_dir(filename, dir, rank, i);
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
      ret = plugin->write_file(filename, buf, file_size, dir, rank, d, f);
      if (ret == 0){
        p_files_created++;
      }else{
        p_files_creation_errors++;
        if (! ignore_precreate_errors){
          printf("Error while creating the file %s (%s)\n", filename, strerror(errno));
          MPI_Abort(MPI_COMM_WORLD, 1);
        }
      }
    }
  }
  free(buf);
}

/* FIFO: create a new file, write to it. Then read from the first created file, delete it... */
void run_benchmark(){
  char filename[FILENAME_MAX];
  int ret;
  char * buf = malloc(file_size);
  memset(buf, rank % 256, file_size);

  for(int f=0; f < num; f++){
    for(int d=0; d < dirs; d++){
      int curRankFolder = (rank + offset * d) % size;

      ret = plugin->write_file(filename, buf, file_size,  dir, curRankFolder, d, precreate + f);
      if (verbosity){
        printf("%d Write %s \n", rank, filename);
      }


      if (ret == MD_ERROR_CREATE){
        printf("Error while creating the file %s (%s)\n", filename, strerror(errno));
        b_file_creation_errors++;
      }else{
        if (ret != MD_SUCCESS){
          printf("Error while writing the file %s (%s)\n", filename, strerror(errno));
          b_file_creation_errors++;
        }else{
          b_file_created++;
        }
      }

      int nextRank = (rank + offset * (d+1)) % size;
      ret = plugin->stat_file(filename, dir, nextRank, d, f);
      if(ret != 0){
        printf("Error while stating the file %s (%s)\n", filename, strerror(errno));
        b_file_access_errors++;
        continue;
      }

      if (verbosity){
        printf("%d Access %s \n", rank, filename);
      }

      ret = plugin->read_file(filename, buf, file_size,  dir, nextRank, d, f);
      if (ret == MD_ERROR_FIND){
        printf("Error while accessing the file %s (%s)\n", filename, strerror(errno));
        b_file_access_errors++;
      }else{
        if (ret != MD_SUCCESS){
          printf("Error while reading the file %s (%s)\n", filename, strerror(errno));
          b_file_access_errors++;
          continue;
        }
        b_file_accessed++;
      }

       plugin->delete_file(filename, dir, nextRank, d, f);
      if (ret != 0){
        b_file_access_errors++;
      }
    }
  }
  free(buf);
}

void run_cleanup(){
  char filename[FILENAME_MAX];
  int ret;

  for(int d=0; d < dirs; d++){
    for(int f=0; f < precreate; f++){
      ret = plugin->delete_file(filename, dir, rank, d, f+num);
      if (ret == 0){
        c_files_deleted++;
      }else{
        c_files_deletion_error++;
      }
    }

    ret = plugin->rm_dir(filename, dir, rank, d);
    if (ret == 0){
      c_dirs_deleted++;
    }
  }
  ret = plugin->rm_rank_dir(filename, dir,rank);
  if (ret == 0){
    c_dirs_deleted++;
  }
}

static void print_additional_thread_report_header(){
  printf("/Pre CreatedDirs CreateFiles ");
  printf("\t/Bench Created Accessed");
  printf("\t/Clean Deleted\n");
}

static void print_additional_thread_reports(char * b){
  b += sprintf(b, "%d\t%.2fs\t\t%.2fs\t\t%.2fs\t", rank, t_precreate_i, t_benchmark_i, t_cleanup_i);
  b += sprintf(b, "     %d(%d)\t%d(%d)", p_dirs_created, p_dirs_creation_errors, p_files_created, p_files_creation_errors);
  b +=  sprintf(b, "\t\t\t%d(%d)\t%d(%d)", b_file_created, b_file_creation_errors, b_file_accessed, b_file_access_errors);
  b += sprintf(b, "\t\t%d(%d)\n", c_files_deleted, c_files_deletion_error);
}

static void prepare_report(){
  int ret;
  double t_max[3] = {t_precreate - t_precreate_i , t_benchmark - t_benchmark_i , t_cleanup - t_cleanup_i};

  uint64_t errors[] = {p_dirs_creation_errors, p_files_creation_errors, b_file_creation_errors, b_file_access_errors, c_files_deletion_error};
  uint64_t correct[] = {p_dirs_created, p_files_created, b_file_created, b_file_accessed, c_files_deleted, c_dirs_deleted};

  if (rank == 0){
    ret = plugin->purge_testdir(dir);
    ret = MPI_Reduce(MPI_IN_PLACE, & t_max, 3, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
    ret = MPI_Reduce(MPI_IN_PLACE, errors, 5, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
    ret = MPI_Reduce(MPI_IN_PLACE, correct, 6, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
    uint64_t sumErrors = errors[0] + errors[1] + errors[2] + errors[3] + errors[4];

    printf("\nTotal runtime: %.3fs precreate: %.2fs benchmark: %.2fs cleanup: %.2fs\n", t_all, t_precreate, t_benchmark, t_cleanup);
    printf("Barrier time:        precreate: %.2fs benchmark: %.2fs cleanup: %.2fs\n", t_max[0], t_max[1], t_max[2]);
    printf("Operations:          /Pre dir: %llu files: %llu /Bench create: %llu access: %llu /Clean files: %llu dirs: %llu\n" , LLU correct[0], LLU correct[1], LLU correct[2], LLU correct[3], LLU correct[4], LLU correct[5]);

    double v_pre = LLU correct[1] / (1024.0*1024) * file_size;
    double v_bench = LLU correct[2] / (1024.0*1024) * file_size;
    printf("Volume:              precreate: %.1f MiB benchmark: %.1f MiB\n", v_pre, v_bench);
    if(sumErrors > 0){
      printf("Errors: %llu /Pre dir: %llu files: %llu /Bench create: %llu access: %llu /Clean files: %llu\n", LLU sumErrors, LLU errors[0], LLU errors[1], LLU errors[2], LLU errors[3], LLU errors[4] );
    }

    printf("\nCompound performance:\n");
    printf("Precreate: %.1f elements/s (dirs+files) %.1f MiB/s ops = (create dir, files, write)\n", (correct[0] + correct[1]) / t_precreate, correct[1] * file_size / t_precreate / (1024.0*1024));
    printf("Benchmark: %.1f iters/s %.1f MiB/s iteration = (write, stat, read, delete)\n", min(correct[2], correct[3]) / t_benchmark, (correct[2] + correct[3]) * file_size / t_benchmark / (1024.0*1024));
    printf("Delete:    %.1f elements/s (dirs+files) ops = (delete dirs, files)\n", (correct[4]+correct[5]) / t_cleanup );

  }else{ // rank != 0
    ret = MPI_Reduce(t_max, NULL, 3, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)

    ret = MPI_Reduce(errors, NULL, 5, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    ret = MPI_Reduce(correct, NULL, 6, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
    CHECK_MPI_RET(ret)
  }

  if( thread_report ){
    // individual reports per thread
    char thread_buffer[4096];

    if (rank == 0){
      printf("\nRank\tPrecreate\tBenchmark\tCleanup\t");
      print_additional_thread_report_header();

      print_additional_thread_reports(thread_buffer);
      printf("%s", thread_buffer);
      for(int i=1; i < size; i++){
        MPI_Recv(thread_buffer, 4096, MPI_CHAR, i, 4711, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("%s", thread_buffer);
      }
    }else{
      print_additional_thread_reports(thread_buffer);
      MPI_Send(thread_buffer, 4096, MPI_CHAR, 0, 4711, MPI_COMM_WORLD);
    }
  }
}


static option_help options [] = {
  {'T', "target", "Target identifier (directory) where to run the benchmark.", OPTION_OPTIONAL_ARGUMENT, 's', & dir},
  {'O', "offset", "Offset in ranks between writers and readers. Writers and readers should be located on different nodes.", OPTION_OPTIONAL_ARGUMENT, 'd', & offset},
  {'I', "interface", "The interface (plugin) to use for the test, use list to show all compiled plugins.", OPTION_OPTIONAL_ARGUMENT, 's', & interface},
  {'N', "num", "Number of I/O operations per thread and directory.", OPTION_OPTIONAL_ARGUMENT, 'd', & num},
  {'P', "precreate", "Number of files to precreate per thread and directory.", OPTION_OPTIONAL_ARGUMENT, 'd', & precreate},
  {'D', "dirs", "Number of directories.", OPTION_OPTIONAL_ARGUMENT, 'd', & dirs},

  {'F', "file-size", "File size for the created files.", OPTION_OPTIONAL_ARGUMENT, 'd', & file_size},

  {0, "skip-cleanup-phase", "Skip the cleanup phase", OPTION_FLAG, 'd', & skip_cleanup},
  {0, "ignore-precreate-errors", "Ignore errors occuring during the pre-creation phase", OPTION_FLAG, 'd', & ignore_precreate_errors},
  {0, "thread-reports", "Independent report per thread", OPTION_FLAG, 'd', & thread_report},

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

  if (argc > parsed){
    parseOptions(argc - parsed, argv + parsed, plugin->get_options(), & printhelp);
  }

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

  size_t total_files_count = dirs * (size_t) (num + precreate) * size;

  if (rank == 0){
    if(num > precreate){
      printf("WARNING: num > precreate, this may cause the situation that no files are available to read\n");
    }
    printf("MD-REAL-IO total files: %zu (version: %s)\n", total_files_count, VERSION);
  }

  timer bench_start;

  if (rank == 0){
    ret = plugin->prepare_testdir(dir);
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

  prepare_report();

  MPI_Finalize();
  return 0;
}
