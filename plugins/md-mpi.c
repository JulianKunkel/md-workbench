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

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h>

#include <plugins/md-mpi.h>

#include <mpi.h>

static char * hint_list = NULL;
static int use_existing_dirs = 0;
static int use_posix_dirs = 0;
static int show_hint_list = 0;
static char * dir = "out";
static MPI_Info info;

static option_help options [] = {
  {'D', "root-dir", "Root directory", OPTION_OPTIONAL_ARGUMENT, 's', & dir},
  {'H', "hints", "List of MPI hints in the format: X=Y|Z=W|...", OPTION_OPTIONAL_ARGUMENT, 's', & hint_list},
  {'S', "show-hints", "Show the einfo MPI hints in the format: X=Y|Z=W|...", OPTION_FLAG, 'd', & show_hint_list},
  {'d', "use-existing-dirs", "Use pre-created directories (since MPI does not support directories); otherwise only a single directory is used", OPTION_FLAG, 'd', & use_existing_dirs},
  {'p', "use-posix-dirs", "Create POSIX directories (since MPI does not support directories)", OPTION_FLAG, 'd', & use_posix_dirs},
  LAST_OPTION
};

static option_help * get_options(){
  return options;
}

static int initialize(){
  if (use_posix_dirs && use_existing_dirs){
    printf("Only one option of -d or -p can be active\n");
    return MD_ERROR_UNKNOWN;
  }

  int ret = MPI_Info_create(& info);
  if (ret != MD_SUCCESS){
    return MD_ERROR_UNKNOWN;
  }
  if (hint_list){
    char * saveptr;
    char * token = strtok_r(hint_list, "|", & saveptr);
    while(token != NULL ){
      char * value = strstr(token, "=");
      value[0] = 0;
      value++;
      //printf("key:%s = %s\n", token, value);
      MPI_Info_set(info, token, value);
      token = strtok_r(NULL, "|", & saveptr);
    }
  }
  return MPI_SUCCESS;
}

static int finalize(){
  MPI_Info_free(& info);
  return MD_SUCCESS;
}


static int prepare_global(){
  if (show_hint_list){
    MPI_Info einfo;
    MPI_File fh;
    char filename[4096];
    sprintf(filename, "%s/dummy", dir);
    int ret = MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_DELETE_ON_CLOSE | MPI_MODE_CREATE | MPI_MODE_WRONLY, info, & fh);
    MPI_File_get_info(fh, & einfo);
    MPI_File_close(& fh);
    if (ret != MPI_SUCCESS){
      return MD_ERROR_UNKNOWN;
    }
    printf("Effective MPI hints: ");
    int count;
    MPI_Info_get_nkeys(einfo, & count);
    for(int i=0; i < count; i++){
      char key[4096];
      char value[4096];
      int flag;
      MPI_Info_get_nthkey(einfo, i, key);
      MPI_Info_get(info, key, 4095, value, & flag);
      if(i > 0){
        printf("|");
      }
      printf("%s=%s", key, value);
    }
    printf("\n");
    MPI_Info_free(& einfo);
  }

  if (use_posix_dirs){
    return mkdir(dir, 0755);
  }else if(use_existing_dirs){
    // check if the directories exist ?
    return MD_NOOP;
  }
  return MD_NOOP;
}

static int purge_global(){
  if (use_posix_dirs){
    return rmdir(dir);
  }
  return MD_NOOP;
}

static int def_dset_name(char * out_name, int n, int d){
  sprintf(out_name, "%s/%d_%d", dir, n, d);
  return MD_SUCCESS;
}

static int def_obj_name(char * out_name, int n, int d, int i){
  sprintf(out_name, "%s/%d_%d/file-%d", dir, n, d, i);
  return MD_SUCCESS;
}

static int create_dset(char * filename){
  if (use_posix_dirs){
    return mkdir(filename, 0755);
  }
  return MD_NOOP;
}

static int rm_dset(char * filename){
  if (use_posix_dirs){
    return rmdir(filename);
  }
  return MD_NOOP;
}

static int write_obj(char * dirname, char * filename, char * buf, size_t file_size){
  int ret;
  MPI_File fh;
  ret = MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_CREATE | MPI_MODE_WRONLY, info, & fh);
  if (ret != MPI_SUCCESS){
    return MD_ERROR_UNKNOWN;
  }

  MPI_Status status;
  MPI_Count count;
  ret = MPI_File_write(fh, buf, file_size, MPI_BYTE, & status);
  MPI_Get_elements_x(& status, MPI_BYTE, & count);
  MPI_File_close(& fh);
  if (ret != MPI_SUCCESS  || (size_t) count != file_size){
    return MD_ERROR_UNKNOWN;
  }

  return MD_SUCCESS;
}



static int read_obj(char * dirname, char * filename, char * buf, size_t file_size){
  int ret;
  MPI_File fh;
  ret = MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, info, & fh);
  if (ret != MPI_SUCCESS){
    return MD_ERROR_FIND;
  }

  MPI_Status status;
  MPI_Count count;
  ret = MPI_File_read(fh, buf, file_size, MPI_BYTE, & status);
  MPI_Get_elements_x(& status, MPI_BYTE, & count);
  MPI_File_close(& fh);
  if (ret != MPI_SUCCESS  || (size_t) count != file_size){
    return MD_ERROR_UNKNOWN;
  }

  return MD_SUCCESS;
}

static int stat_obj(char * dirname, char * filename, size_t file_size){
  int ret;
  MPI_File fh;
  ret = MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, info, & fh);
  if (ret != MPI_SUCCESS){
    return MD_ERROR_UNKNOWN;
  }

  MPI_Offset size;
  ret = MPI_File_get_size(fh, & size);
  MPI_File_close(& fh);
  if (ret != MPI_SUCCESS  || (size_t) size != file_size){
    return MD_ERROR_UNKNOWN;
  }
  return MD_SUCCESS;
}

static int delete_obj(char * dirname, char * filename){
  int ret = MPI_File_delete(filename, MPI_INFO_NULL);
  return ret == MPI_SUCCESS ? MD_SUCCESS : MD_ERROR_UNKNOWN;
}




struct md_plugin md_plugin_mpi = {
  "mpiio",
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
