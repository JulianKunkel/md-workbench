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


#include <time.h>
#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include <md_util.h>
#include <md_option.h>

#include <plugins/md-plugin.h>

#include <plugins/md-dummy.h>
#include <plugins/md-posix.h>
#include <plugins/md-postgres.h>
#include <plugins/md-mongo.h>
#include <plugins/md-s3.h>
#include <plugins/md-mpi.h>

struct md_plugin * md_plugin_list[] = {
& md_plugin_dummy,
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

// successfull, errors
typedef struct {
  int suc;
  int err;
} op_stat_t;

// statistics for running a single phase
typedef struct{ // NOTE: if this type is changed, adjust end_phase() !!!
  double t;
  double t_incl_barrier;

  op_stat_t dset_name;
  op_stat_t dset_create;
  op_stat_t dset_delete;

  op_stat_t obj_name;
  op_stat_t obj_create;
  op_stat_t obj_read;
  op_stat_t obj_stat;
  op_stat_t obj_delete;
} phase_stat_t;

#define init_stats(p) memset(& p, 0, sizeof(p));

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

  int limit_memory;
  int limit_memory_between_phases;

  int verbosity;
  int process_report;
  int print_pattern;
  int print_detailed_stats;
  int quiet_output;

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
  o.iterations = 3;
  o.file_size = 3900;
}

static void print_detailed_stat_header(){
    printf("phase\t\td name\tcreate\tdelete\tob nam\tcreate\tread\tstat\tdelete\tt_inc_b\tt_no_bar\tthp\n");
}

static int sum_err(phase_stat_t * p){
  return p->dset_name.err + p->dset_create.err +  p->dset_delete.err + p->obj_name.err + p->obj_create.err + p->obj_read.err + p->obj_stat.err + p->obj_delete.err;
}

static void print_p_stat(char * buff, const char * name, phase_stat_t * p, double t){
  const double tp = (uint64_t)(p->obj_create.suc + p->obj_read.suc) * o.file_size / t / 1024 / 1024;

  const int errs = sum_err(p);

  if (o.print_detailed_stats){
    sprintf(buff, "%s \t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.3fs\t%.3fs\t%.2f MiB/s", name, p->dset_name.suc, p->dset_create.suc,  p->dset_delete.suc, p->obj_name.suc, p->obj_create.suc, p->obj_read.suc,  p->obj_stat.suc, p->obj_delete.suc, p->t, t, tp);

    if (errs > 0){
      sprintf(buff, "%s err\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d", name, p->dset_name.err, p->dset_create.err,  p->dset_delete.err, p->obj_name.err, p->obj_create.err, p->obj_read.err, p->obj_stat.err, p->obj_delete.err);
    }
  }else{
    int pos = 0;
    // single line
    switch(name[0]){
      case('b'):
        pos = sprintf(buff, "%s %.1fs %d obj %.1f obj/s %.1f Mib/s", name, t,
          p->obj_create.suc,
          p->obj_create.suc / t,
          tp);
        break;
      case('p'):
        pos = sprintf(buff, "%s %.1fs %d dset %d obj %.3f dset/s %.1f obj/s %.1f Mib/s", name, t,
          p->dset_create.suc,
          p->obj_create.suc,
          p->dset_create.suc / t,
          p->obj_create.suc / t,
          tp);
        break;
      case('c'):
        pos = sprintf(buff, "%s %.1fs %d obj %d dset %.1f obj/s %.3f dset/s", name, t,
          p->obj_delete.suc,
          p->dset_delete.suc,
          p->obj_delete.suc / t,
          p->dset_delete.suc / t);
        break;
      default:
        pos = sprintf(buff, "%s: unknown phase", name);
      break;
    }
    if(! o.quiet_output || errs > 0){
      pos = pos + sprintf(buff + pos, " (%d errs", errs);
      if(errs > 0){
        sprintf(buff + pos, "!!!)" );
      }else{
        sprintf(buff + pos, ")" );
      }
    }
  }
}

static void end_phase(const char * name, phase_stat_t * p, timer start){
  int ret;
  char buff[4096];

  char * limit_memory_P = NULL;
  p->t = stop_timer(start);
  MPI_Barrier(MPI_COMM_WORLD);
  p->t_incl_barrier = stop_timer(start);

  // prepare the summarized report
  phase_stat_t g_stat;
  init_stats(g_stat);
  // reduce timers
  ret = MPI_Reduce(& p->t, & g_stat.t, 2, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  CHECK_MPI_RET(ret)
  ret = MPI_Reduce(& p->dset_name, & g_stat.dset_name, 2*(3+5), MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  CHECK_MPI_RET(ret)

  if (o.rank == 0){
    //print the stats:
    print_p_stat(buff, name, & g_stat, g_stat.t_incl_barrier);
    printf("%s\n", buff);
  }

  if(o.process_report){
    if(o.rank == 0){
      print_p_stat(buff, name, p, p->t);
      printf("0: %s\n", buff);
      for(int i=1; i < o.size; i++){
        MPI_Recv(buff, 4096, MPI_CHAR, i, 4711, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("%d: %s\n", i, buff);
      }
    }else{
      print_p_stat(buff, name, p, p->t);
      MPI_Send(buff, 4096, MPI_CHAR, 0, 4711, MPI_COMM_WORLD);
    }
  }

  // allocate if necessary
  ret = mem_preallocate(& limit_memory_P, o.limit_memory_between_phases, o.verbosity >= 3);
  if( ret != 0){
    printf("%d: Error allocating memory!\n", o.rank);
  }
  mem_free_preallocated(& limit_memory_P);
}

void run_precreate(phase_stat_t * s){
  char dset[4096];
  char obj_name[4096];
  int ret;

  for(int i=0; i < o.dset_count; i++){
    ret = o.plugin->def_dset_name(dset, o.rank, i);
    if (ret != MD_SUCCESS){
      if (! o.ignore_precreate_errors){
        printf("Error defining the dataset name\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
      s->dset_name.err++;
      continue;
    }
    s->dset_name.suc++;
    ret = o.plugin->create_dset(dset);
    if (ret == MD_NOOP){
      // do not increment any counter
    }else if (ret == MD_SUCCESS){
      s->dset_create.suc++;
    }else{
      s->dset_create.err++;
      if (! o.ignore_precreate_errors){
        printf("%d: Error while creating the dset: %s\n", o.rank, dset);
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
  }

  char * buf = malloc(o.file_size);
  memset(buf, o.rank % 256, o.file_size);

  // create the obj
  for(int d=0; d < o.dset_count; d++){
    ret = o.plugin->def_dset_name(dset, o.rank, d);
    for(int f=0; f < o.precreate; f++){
      ret = o.plugin->def_obj_name(obj_name, o.rank, d, f);
      if (ret != MD_SUCCESS){
        s->dset_name.err++;
        if (! o.ignore_precreate_errors){
          printf("%d: Error while creating the obj name\n", o.rank);
          MPI_Abort(MPI_COMM_WORLD, 1);
        }
        s->obj_name.err++;
        continue;
      }
      ret = o.plugin->write_obj(dset, obj_name, buf, o.file_size);
      if (ret == MD_NOOP){
        // do not increment any counter
      }else if (ret == MD_SUCCESS){
        s->obj_create.suc++;
      }else{
        s->obj_create.err++;
        if (! o.ignore_precreate_errors){
          printf("%d: Error while creating the obj: %s\n", o.rank, obj_name);
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
void run_benchmark(phase_stat_t * s, int start_index){
  char dset[4096];
  char obj_name[4096];
  int ret;
  char * buf = malloc(o.file_size);
  memset(buf, o.rank % 256, o.file_size);

  for(int f=0; f < o.num; f++){
    for(int d=0; d < o.dset_count; d++){
      int writeRank = (o.rank + o.offset * (d+1)) % o.size;
      const int prevFile = f + start_index;
      ret = o.plugin->def_obj_name(obj_name, writeRank, d, o.precreate + prevFile);
      if (ret != MD_SUCCESS){
        s->obj_name.err++;
        continue;
      }
      ret = o.plugin->def_dset_name(dset, writeRank, d);

      if (o.verbosity >= 2)
        printf("%d write %s:%s \n", o.rank, dset, obj_name);

      ret = o.plugin->write_obj(dset, obj_name, buf, o.file_size);
      if (ret == MD_SUCCESS){
          s->obj_create.suc++;
      }else if (ret == MD_ERROR_CREATE){
        if (o.verbosity)
          printf("%d: Error while creating the obj: %s\n",o.rank, dset);
        s->obj_create.err++;
      }else if (ret == MD_NOOP){
          // do not increment any counter
      }else{
        if (o.verbosity)
          printf("%d: Error while writing the obj: %s\n",o.rank, dset);
        s->obj_create.err++;
      }

      int readRank = (o.rank - o.offset * (d+1)) % o.size;
      readRank = readRank < 0 ? readRank + o.size : readRank;
      ret = o.plugin->def_obj_name(obj_name, readRank, d, prevFile);
      if (ret != MD_SUCCESS){
        s->obj_name.err++;
        continue;
      }
      ret = o.plugin->def_dset_name(dset, readRank, d);

      if (o.verbosity >= 2){
        printf("%d: stat %s:%s \n", o.rank, dset, obj_name);
      }
      ret = o.plugin->stat_obj(dset, obj_name, o.file_size);
      if(ret != MD_SUCCESS && ret != MD_NOOP){
        if (o.verbosity)
          printf("%d: Error while stating the obj: %s\n", o.rank, dset);
        s->obj_stat.err++;
        continue;
      }
      s->obj_stat.suc++;

      if (o.verbosity >= 2){
        printf("%d: read %s:%s \n", o.rank, dset, obj_name);
      }
      ret = o.plugin->read_obj(dset, obj_name, buf, o.file_size);
      if (ret == MD_SUCCESS){
        s->obj_read.suc++;
      }else if (ret == MD_NOOP){
        // nothing to do
      }else if (ret == MD_ERROR_FIND){
        printf("%d: Error while accessing the file %s (%s)\n", o.rank, dset, strerror(errno));
        s->obj_read.err++;
      }else{
        printf("%d: Error while reading the file %s (%s)\n", o.rank, dset, strerror(errno));
        s->obj_read.err++;
      }

      if (o.verbosity >= 2){
        printf("%d: delete %s:%s \n", o.rank, dset, obj_name);
      }
      o.plugin->delete_obj(dset, obj_name);
      if (ret == MD_SUCCESS){
        s->obj_delete.suc++;
      }else if (ret == MD_NOOP){
        // nothing to do
      }else{
        printf("%d: Error while deleting the object %s:%s\n", o.rank, dset, obj_name);
        s->obj_delete.err++;
      }
    }
  }
  free(buf);
}

void run_cleanup(phase_stat_t * s, int start_index){
  char dset[4096];
  char obj_name[4096];
  int ret;

  for(int d=0; d < o.dset_count; d++){
    ret = o.plugin->def_dset_name(dset, o.rank, d);

    for(int f=0; f < o.precreate; f++){
      ret = o.plugin->def_obj_name(obj_name, o.rank, d, f + o.num + start_index);
      ret = o.plugin->delete_obj(dset, obj_name);
      if (ret == MD_NOOP){
        // nothing to do
      }else if (ret == MD_SUCCESS){
        s->obj_delete.suc++;
      }else if(ret != MD_NOOP){
        s->obj_delete.err++;
      }
    }

    ret = o.plugin->rm_dset(dset);
    if (ret == MD_SUCCESS){
      s->dset_delete.suc++;
    }else if (ret != MD_NOOP){
      s->dset_delete.err++;
    }
  }
}


static option_help options [] = {
  {'O', "offset", "Offset in o.ranks between writers and readers. Writers and readers should be located on different nodes.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.offset},
  {'i', "interface", "The interface (plugin) to use for the test, use list to show all compiled plugins.", OPTION_OPTIONAL_ARGUMENT, 's', & o.interface},
  {'I', "obj-per-proc", "Number of I/O operations per process and data set.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.num},
  {'P', "precreate-per-set", "Number of object to precreate per process and data set.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.precreate},
  {'D', "data-sets", "Number of data sets and communication neighbors per iteration.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.dset_count},
  {'q', "quiet", "Avoid irrelevant printing.", OPTION_FLAG, 'd', & o.quiet_output},
  {'m', "lim-free-mem", "Allocate memory until this limit (in MiB) is reached.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.limit_memory},
  {'M', "lim-free-mem-phase", "Allocate memory until this limit (in MiB) is reached between the phases, but free it before starting the next phase; the time is NOT included for the phase.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.limit_memory_between_phases},
  {0, "print-detailed-stats", "Print detailed machine parsable statistics.", OPTION_FLAG, 'd', & o.print_detailed_stats},
  {0, "print-pattern", "Print the pattern, the neighbors used in one iteration.", OPTION_FLAG, 'd', & o.print_pattern},
  {'S', "object-size", "Size for the created objects.", OPTION_OPTIONAL_ARGUMENT, 'd', & o.file_size},
  {'R', "iterations", "Rerun the main phase multiple times", OPTION_OPTIONAL_ARGUMENT, 'd', & o.iterations},
  {0, "start-index", "Start with this file index in the benchmark; if combined with run-benchmark, this allows to run the benchmark multiple times", OPTION_OPTIONAL_ARGUMENT, 'd', & o.start_index},
  {'1', "run-precreate", "Run precreate phase", OPTION_FLAG, 'd', & o.phase_precreate},
  {'2', "run-benchmark", "Run benchmark phase", OPTION_FLAG, 'd', & o.phase_benchmark},
  {'3', "run-cleanup", "Run cleanup phase (only run explicit phases)", OPTION_FLAG, 'd', & o.phase_cleanup},
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
    if((*p_it)->name == NULL){
      printf("Error, module \"%s\" not linked properly\n", o.interface);
      MPI_Abort(MPI_COMM_WORLD, 1);
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
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }
  MPI_Finalize();
  exit(0);
}

static void printTime(){
    char buff[100];
    time_t now = time(0);
    strftime (buff, 100, "%Y-%m-%d %H:%M:%S", localtime (&now));
    printf("%s\n", buff);
}

int main(int argc, char ** argv){
  int ret;
  int printhelp = 0;
  char * limit_memory_P = NULL;

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
    printf("%d: Error initializing module\n", o.rank);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  size_t total_obj_count = o.dset_count * (size_t) (o.num * o.iterations + o.precreate) * o.size;
  if (o.rank == 0 && ! o.quiet_output){
    printf("MD-REAL-IO total objects: %zu workingset size: %.3f MiB (version: %s) time: ", total_obj_count, o.size * o.dset_count * o.precreate * o.file_size / 1024.0 / 1024.0,  VERSION);
    printTime();
    if(o.num > o.precreate){
      printf("WARNING: num > precreate, this may cause the situation that no objects are available to read\n");
    }
  }

  if ( o.rank == 0 && ! o.quiet_output ){
    // print the set output options
    print_current_options(options);
    printf("\n");
    print_current_options(o.plugin->get_options());

    printf("\n");
  }

  // preallocate memory if necessary
  ret = mem_preallocate(& limit_memory_P, o.limit_memory, o.verbosity >= 3);
  if(ret != 0){
    printf("%d: Error allocating memory\n", o.rank);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  timer bench_start;
  start_timer(& bench_start);
  timer tmp;
  phase_stat_t phase_stats;

  if(o.rank == 0 && o.print_detailed_stats && ! o.quiet_output){
    print_detailed_stat_header();
  }

  if (o.phase_precreate){
    if (o.rank == 0){
      ret = o.plugin->prepare_global();
      if ( ret != MD_SUCCESS && ret != MD_NOOP ){
        printf("Rank 0 could not prepare the run, aborting\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
    init_stats(phase_stats);
    MPI_Barrier(MPI_COMM_WORLD);

    // pre-creation phase
    start_timer(& tmp);
    run_precreate(& phase_stats);
    end_phase("precreate", & phase_stats, tmp);
  }

  int current_index = o.start_index;

  if (o.phase_benchmark){
    // benchmark phase
    for(int i=0; i < o.iterations; i++){
      init_stats(phase_stats);
      start_timer(& tmp);
      run_benchmark(& phase_stats, current_index);
      current_index += o.num;
      end_phase("benchmark", & phase_stats, tmp);
    }
    current_index -= o.num;
  }

  // cleanup phase
  if (o.phase_cleanup){
    init_stats(phase_stats);
    start_timer(& tmp);
    run_cleanup(& phase_stats, current_index);
    end_phase("cleanup", & phase_stats, tmp);

    if (o.rank == 0){
      ret = o.plugin->purge_global();
      if (ret != MD_SUCCESS && ret != MD_NOOP){
        printf("Rank 0: Error purging the global environment\n");
      }
    }
  }

  double t_all = stop_timer(bench_start);
  ret = o.plugin->finalize();
  if (ret != MD_SUCCESS){
    printf("Error while finalization of module\n");
  }
  if (o.rank == 0 && ! o.quiet_output){
    printf("Total runtime: %.0fs time: ",  t_all);
    printTime();
  }

  mem_free_preallocated(& limit_memory_P);

  MPI_Finalize();
  return 0;
}
