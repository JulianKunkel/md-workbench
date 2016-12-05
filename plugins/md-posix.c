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

#include <plugins/md-posix.h>


static int prepare_testdir(char * dir){
  return mkdir(dir, 0755);
}

static int purge_testdir(char * dir){
  return rmdir(dir);
}

static int create_rank_dir(char * filename, char * prefix, int rank){
  sprintf(filename, "%s/%d", prefix, rank);
  return mkdir(filename, 0755);
}

static int rm_rank_dir(char * filename, char * prefix, int rank){
  sprintf(filename, "%s/%d", prefix, rank);
  return rmdir(filename);
}


static int create_dir(char * filename, char * prefix, int rank, int iteration){
  sprintf(filename, "%s/%d/%d", prefix, rank, iteration);
  return mkdir(filename, 0755);
}

static int rm_dir(char * filename, char * prefix, int rank, int iteration){
  sprintf(filename, "%s/%d/%d", prefix, rank, iteration);
  return rmdir(filename);
}

static int write_file(char * filename, char * buf, size_t file_size,  char * prefix, int rank, int dir, int iteration){
  int ret;
  int fd;
  sprintf(filename, "%s/%d/%d/file-%d", prefix, rank, dir, iteration);
  fd = open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
  if (fd == -1) return MD_ERROR_CREATE;
  ret = write(fd, buf, file_size);
  ret = (ret == file_size) ? MD_SUCCESS: MD_UNKNOWN;
  close(fd);
  return ret;
}


static int read_file(char * filename, char * buf, size_t file_size,  char * prefix, int rank, int dir, int iteration){
  int fd;
  int ret;
  sprintf(filename, "%s/%d/%d/file-%d", prefix, rank, dir, iteration);
  fd = open(filename, O_RDWR);
  if (fd == -1) return MD_ERROR_FIND;
  ret = read(fd, buf, file_size);
  ret = (ret == file_size) ? MD_SUCCESS: MD_UNKNOWN;
  close(fd);
  return ret;
}

static int stat_file(char * filename, char * prefix, int rank, int dir, int iteration){
  struct stat file_stats;
  int ret;
  ret = stat(filename, & file_stats);
  if ( ret != 0 ){
    return MD_ERROR_FIND;
  }
  return MD_SUCCESS;
}

static int delete_file(char * filename, char * prefix, int rank, int dir, int iteration){
  sprintf(filename, "%s/%d/%d/file-%d", prefix, rank, dir, iteration);
  return unlink(filename);
}




struct md_plugin md_plugin_posix = {
  "posix",
  prepare_testdir,
  purge_testdir,
  create_rank_dir,
  rm_rank_dir,
  create_dir,
  rm_dir,
  write_file,
  read_file,
  stat_file,
  delete_file
};
