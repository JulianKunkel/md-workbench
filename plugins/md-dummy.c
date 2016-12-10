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

#include <stdio.h>

#include <plugins/md-dummy.h>

static int fake_errors = 0;

static option_help options [] = {
  {'f', "fake-errors", "Fake errors while running benchmark, best to use with --ignore-precreate-errors.", OPTION_FLAG, 'd', & fake_errors},
  LAST_OPTION
};

static option_help * get_options(){
  return options;
}

static int initialize(){
  return MD_SUCCESS;
}

static int finalize(){
  return MD_SUCCESS;
}


static int prepare_global(){
  return MD_SUCCESS;
}

static int purge_global(){
  return MD_SUCCESS;
}

static int def_dset_name(char * out_name, int n, int d){
  sprintf(out_name, "n=%d/d=%d", n, d);
  return MD_SUCCESS;
}

static int def_obj_name(char * out_name, int n, int d, int i){
  sprintf(out_name, "n=%d/d=%d/i=%d", n, d, i);
  return MD_SUCCESS;
}

static int create_dset(char * filename){
  return MD_SUCCESS;
}

static int rm_dset(char * filename){
  return MD_SUCCESS;
}

static int write_obj(char * dirname, char * filename, char * buf, size_t file_size){
  if(fake_errors){
    return MD_ERROR_UNKNOWN;
  }
  return MD_SUCCESS;
}


static int read_obj(char * dirname, char * filename, char * buf, size_t file_size){
  if(fake_errors){
    return MD_ERROR_UNKNOWN;
  }
  return MD_SUCCESS;
}

static int stat_obj(char * dirname, char * filename, size_t file_size){
  if(fake_errors){
    return MD_ERROR_FIND;
  }
  return MD_SUCCESS;
}

static int delete_obj(char * dirname, char * filename){
  if(fake_errors){
    return MD_ERROR_UNKNOWN;
  }
  return MD_SUCCESS;
}




struct md_plugin md_plugin_dummy = {
  "dummy",
  get_options,
  initialize,
  finalize,
  prepare_global,
  purge_global,

  def_dset_name,
  create_dset,
  rm_dset,

  def_obj_name,
  write_obj,
  read_obj,
  stat_obj,
  delete_obj
};
