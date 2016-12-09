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

#include <libs3.h>

#include <plugins/md-s3.h>

static int bucket_per_set = 0;
static char * access_key = NULL;
static char * secret_key = NULL;
static char * host = NULL;

static option_help options [] = {
  {'b', "bucket-per-set", "Use one bucket to map a set, otherwise only one bucket is used.", OPTION_FLAG, 'd', & bucket_per_set},
  {'H', "host", "The host.", OPTION_REQUIRED_ARGUMENT, 's', & host},
  {'s', "secret-key", "The secret key.", OPTION_REQUIRED_ARGUMENT, 's', & secret_key},
  {'a', "access-key", "The access key.", OPTION_REQUIRED_ARGUMENT, 's', & access_key},
  LAST_OPTION
};

static option_help * get_options(){
  return options;
}

static int initialize(){
  int ret = S3_initialize("s3", S3_INIT_ALL, host);
  if ( ret == S3StatusOK ){
    return MD_SUCCESS;
  }
  printf("Error in s3: %s\n", S3_get_status_name(ret));
  return MD_ERROR_UNKNOWN;
}

static int finalize(){
  S3_deinitialize();
  return MD_SUCCESS;
}


static int prepare_global(){
  if (! bucket_per_set){
    // check if the bucket exists, otherwise create it

    // S3_test_bucket
    // S3_create_bucket

    // S3_MAX_BUCKET_NAME_SIZE
    // S3_MAX_KEY_SIZE
  }

  return MD_NOOP;
}

static int purge_global(){
  if (! bucket_per_set){
    // S3_delete_bucket
  }
  return rmdir(dir);
}

static int create_dir(char * filename, char * prefix, int rank, int iteration){
  if (bucket_per_set){
    // create bucket
  }else{
    return MD_NOOP;
  }

  sprintf(filename, "%s/%d/%d", prefix, rank, iteration);
  return mkdir(filename, 0755);
}

static int rm_dir(char * filename, char * prefix, int rank, int iteration){
  if (bucket_per_set){
    // delete bucket
  }else{
    return MD_NOOP;
  }

  sprintf(filename, "%s/%d/%d", prefix, rank, iteration);
  return rmdir(filename);
}

static S3BucketContext * chooseBucket(char * prefix, int rank, int dir, int iteration){
  if (bucket_per_set){
    // choose the set bucket
  }else{
    // choose the global bucket
  }
}

static int write_file(char * filename, char * buf, size_t file_size,  char * prefix, int rank, int dir, int iteration){
  S3BucketContext * bucket = chooseBucket(prefix, rank, dir, iteration);
  // S3_put_object
  int ret;
  int fd;
  sprintf(filename, "%s/%d/%d/file-%d", prefix, rank, dir, iteration);
  fd = open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
  if (fd == -1) return MD_ERROR_CREATE;
  ret = write(fd, buf, file_size);
  ret = ( (size_t) ret == file_size) ? MD_SUCCESS: MD_ERROR_UNKNOWN;
  close(fd);
  return ret;
}


static int read_file(char * filename, char * buf, size_t file_size,  char * prefix, int rank, int dir, int iteration){
  S3BucketContext * bucket = chooseBucket(prefix, rank, dir, iteration);
  // S3_get_object
  int fd;
  int ret;
  sprintf(filename, "%s/%d/%d/file-%d", prefix, rank, dir, iteration);
  fd = open(filename, O_RDWR);
  if (fd == -1) return MD_ERROR_FIND;
  ret = read(fd, buf, file_size);
  ret = ( (size_t) ret == file_size) ? MD_SUCCESS: MD_ERROR_UNKNOWN;
  close(fd);
  return ret;
}

static int stat_file(char * filename, char * prefix, int rank, int dir, int iteration, int file_size){
  S3BucketContext * bucket = chooseBucket(prefix, rank, dir, iteration);
  // how to ? Should use HEAD request, S3_head_object (?) or use S3_get_object with size = 1, offset = 0 ?
  struct stat file_stats;
  int ret;
  sprintf(filename, "%s/%d/%d/file-%d", prefix, rank, dir, iteration);
  ret = stat(filename, & file_stats);
  if ( ret != 0 ){
    return MD_ERROR_FIND;
  }
  return MD_SUCCESS;
}

static int delete_file(char * filename, char * prefix, int rank, int dir, int iteration){
  S3BucketContext * bucket = chooseBucket(prefix, rank, dir, iteration);
  // S3_delete_object
  sprintf(filename, "%s/%d/%d/file-%d", prefix, rank, dir, iteration);
  return unlink(filename);
}




struct md_plugin md_plugin_s3 = {
  "s3",
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
