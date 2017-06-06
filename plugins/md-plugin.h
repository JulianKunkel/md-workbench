// This object is part of MD-REAL-IO.
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

#include <stdlib.h>

#include <md_option.h>

struct md_plugin{
  char * name; // the name of the plugin, needed for -I option

  option_help *  (*get_options)();
  // called by each process to init/finalize the plugin
  int (*initialize)();
  int (*finalize)();

  // rank0 calls these methods to create / purge the initial setup:
  int (*prepare_global)();
  int (*purge_global)();
  // the position of the object index, if run multiple times
  int (*return_position)();
  void (*store_position)(int position);

  // each process may need to create / delete a set of dat sets
  int (*def_dset_name)(char * out_name, int n, int d);

  // before calling these functions, the name must be set using def_dset_name
  int (*create_dset)(char * name);
  int (*rm_dset)(char * name);

  // n  == rank, d == data set id, i = iteration
  int (*def_obj_name)(char * out_name, int n, int d, int i);

  // before calling these functions, use def_obj_name to set the object name
  // actually used during the benchmark to access and delete objects
  int (*write_obj)(char * dset, char * name, char * buf, size_t size);
  int (*read_obj)(char * dset, char * name, char * buf, size_t size);
  int (*stat_obj)(char * dset, char * name, size_t object_size);
  int (*delete_obj)(char * dset, char * name);
};

enum MD_ERROR{
  MD_ERROR_UNKNOWN = -1,
  MD_SUCCESS = 0,
  MD_ERROR_CREATE,
  MD_ERROR_FIND,
  MD_NOOP, // this is returned, if the implementation doesn't do anything
};

#endif
