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

#ifndef MD_PLUGIN_H
#define MD_PLUGIN_H

#include <md_option.h>

struct md_plugin{
  char * name; // the name of the plugin, needed for -I option

  option_help *  (*get_options)();
  // rank0 calls these methods to create / purge the initial setup:
  int (*prepare_testdir)(char * dir);
  int (*purge_testdir)(char * dir);

  // each process creates / deletes a set of entities
  int (*create_rank_dir)(char * out_filename, char * dir, int rank);
  int (*rm_rank_dir)(char * out_filename, char * dir, int rank);

  int (*create_dir)(char * out_filename, char * prefix, int rank, int iteration);
  int (*rm_dir)(char * out_filename, char * prefix, int rank, int iteration);

  // actually used during the benchmark to access and delete objects
  int (*write_file)(char * out_filename, char * buf, size_t size, char * prefix, int rank, int dir, int iteration);
  int (*read_file)(char * out_filename, char * buf, size_t size, char * prefix, int rank, int dir, int iteration);
  int (*stat_file)(char * out_filename, char * prefix, int rank, int dir, int iteration);
  int (*delete_file)(char * out_filename, char * prefix, int rank, int dir, int iteration);
};

enum MD_ERROR{
  MD_UNKNOWN = -1,
  MD_SUCCESS = 0,
  MD_ERROR_CREATE,
  MD_ERROR_FIND
};

#ifdef MD_PLUGIN_POSIX
#include <plugins/md-posix.h>
#endif

#endif
