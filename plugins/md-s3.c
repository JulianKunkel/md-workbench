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

#include <libs3.h>

#include <plugins/md-s3.h>

static int bucket_per_set = 0;
static char * access_key = NULL;
static char * secret_key = NULL;
static char * host = NULL;
static char * bucket_prefix = "mdrealio";
static char * locationConstraint = NULL;

static int dont_suffix = 0;

static S3BucketContext bucket_context = {NULL};

static option_help options [] = {
  {'b', "bucket-per-set", "Use one bucket to map a set, otherwise only one bucket is used.", OPTION_FLAG, 'd', & bucket_per_set},
  {'B', "bucket-name-prefix", "The name of the bucket (when using without -b), otherwise it is used as prefix.", OPTION_OPTIONAL_ARGUMENT, 's', & bucket_prefix},
  {'p', "dont-suffix-bucket", "If not selected, then a hash will be added to the bucket name to increase uniqueness.", OPTION_FLAG, 'd', & dont_suffix },
  {'H', "host", "The host.", OPTION_OPTIONAL_ARGUMENT, 's', & host},
  {'s', "secret-key", "The secret key.", OPTION_REQUIRED_ARGUMENT, 'H', & secret_key},
  {'a', "access-key", "The access key.", OPTION_REQUIRED_ARGUMENT, 'H', & access_key},
  LAST_OPTION
};

static option_help * get_options(){
  return options;
}



static int initialize(){
  int ret = S3_initialize(NULL, S3_INIT_ALL, host);

  // create a bucket id based on access-key using a trivial checksumming
  if(! dont_suffix){
    uint64_t c = 0;
    char * r = access_key;
    for(uint64_t pos = 1; (*r) != '\0' ; r++, pos*=10) {
      c += (*r) * pos;
    }
    int count = snprintf(NULL, 0, "%s%lu", bucket_prefix, c);
    char * old_prefix = bucket_prefix;
    bucket_prefix = malloc(count + 1);
    sprintf(bucket_prefix, "%s%lu", old_prefix, c);
  }

  // init bucket context
  memset(&bucket_context, 0, sizeof(bucket_context));
  bucket_context.hostName = host;
  bucket_context.bucketName = bucket_prefix;
  bucket_context.protocol = S3ProtocolHTTP;
  bucket_context.uriStyle = S3UriStylePath;
  bucket_context.accessKeyId = access_key;
  bucket_context.secretAccessKey = secret_key;

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

static int current_index(){
  return 0;
}
static void store_position(int pos){
  return;
}

static int def_dset_name(char * out_name, int n, int d){
  // S3_MAX_BUCKET_NAME_SIZE
  if (bucket_per_set){
    sprintf(out_name, "%sx%dx%d", bucket_prefix, n, d);
  }else{
    sprintf(out_name, "%s", bucket_prefix);
  }
  return MD_SUCCESS;
}

static int def_obj_name(char * out_name, int n, int d, int i){
  // S3_MAX_KEY_SIZE
  if (bucket_per_set){
    sprintf(out_name, "%d", i);
  }else{
    sprintf(out_name, "%d_%d_%d", n, d, i);
  }
  return MD_SUCCESS;
}


static S3Status s3status = S3StatusInterrupted;
static S3ErrorDetails s3error = {NULL};

static S3Status responsePropertiesCallback(const S3ResponseProperties *properties, void *callbackData){
  s3status = S3StatusOK;
  return s3status;
}

static void responseCompleteCallback(S3Status status, const S3ErrorDetails *error, void *callbackData) {
  s3status = status;
  if (error == NULL){
    s3error.message = NULL;
  }else{
    s3error = *error;
  }
  return;
}

#define CHECK_ERROR if (s3status != S3StatusOK){ printf("Error \"%s\": %s - %s\n", S3_get_status_name(s3status), s3error.message, s3error.furtherDetails); return MD_ERROR_UNKNOWN; }

static S3ResponseHandler responseHandler = {  &responsePropertiesCallback, &responseCompleteCallback };

static int prepare_global(){
  if (! bucket_per_set){
    // check if the bucket exists, otherwise create it

    S3_test_bucket(S3ProtocolHTTP, S3UriStylePath, access_key, secret_key, NULL, bucket_prefix, S3CannedAclPrivate, locationConstraint, NULL,  & responseHandler, NULL);
    if (s3status != S3StatusErrorNoSuchBucket){
       printf("Error, the bucket %s already exists\n", bucket_prefix);
       return MD_ERROR_UNKNOWN;
    }
    S3_create_bucket(S3ProtocolHTTP, access_key, secret_key, NULL, bucket_prefix, S3CannedAclPrivate, locationConstraint, NULL,  & responseHandler, NULL);
    CHECK_ERROR
    return MD_SUCCESS;
  }

  return MD_NOOP;
}

static int purge_global(){
  if (! bucket_per_set){
    S3_delete_bucket(S3ProtocolHTTP, S3UriStylePath, access_key, secret_key, NULL, bucket_prefix, NULL,  & responseHandler, NULL);
    CHECK_ERROR
    return MD_SUCCESS;
  }
  return MD_NOOP;
}


static int create_dset(char * name){
  if (bucket_per_set){
    S3_create_bucket(S3ProtocolHTTP, access_key, secret_key, NULL, name, S3CannedAclPrivate, locationConstraint, NULL,  & responseHandler, NULL);
    CHECK_ERROR
    return MD_SUCCESS;
  }else{
    return MD_NOOP;
  }
}

static int rm_dset(char * name){
  if (bucket_per_set){
    S3_delete_bucket(S3ProtocolHTTP, S3UriStylePath, access_key, secret_key, NULL, name, NULL,  & responseHandler, NULL);
    CHECK_ERROR
    return MD_SUCCESS;
  }else{
    return MD_NOOP;
  }
}

S3BucketContext * getBucket(char * objname){
  if (bucket_per_set){
    // choose the set bucket
    bucket_context.bucketName = objname;
    return & bucket_context;
  }else{
    // choose the global bucket
    return & bucket_context;
  }
}

struct data_handling{
  char * buf;
  int64_t size;
};

static int putObjectDataCallback(int bufferSize, char *buffer, void *callbackData){
  struct data_handling * dh = (struct data_handling *) callbackData;
  const int64_t size = dh->size > bufferSize ? bufferSize : dh->size;
  memcpy(buffer, dh->buf, size);
  dh->buf += size;
  dh->size -= size;

  return size;
}

static S3PutObjectHandler putObjectHandler = { {  &responsePropertiesCallback, &responseCompleteCallback }, & putObjectDataCallback };

static int write_obj(char * bucket_name, char * obj_name, char * buf, size_t obj_size){
  struct data_handling dh = { .buf = buf, .size = obj_size };
  S3BucketContext * bucket = getBucket(bucket_name);
  S3_put_object(bucket, obj_name, obj_size, NULL, NULL, &putObjectHandler, & dh);
  CHECK_ERROR

  return MD_SUCCESS;
}

static S3Status getObjectDataCallback(int bufferSize, const char *buffer,  void *callbackData){
  struct data_handling * dh = (struct data_handling *) callbackData;
  const int64_t size = dh->size > bufferSize ? bufferSize : dh->size;
  memcpy(dh->buf, buffer, size);
  dh->buf += size;
  dh->size -= size;

  return S3StatusOK;
}

static S3GetObjectHandler getObjectHandler = { {  &responsePropertiesCallback, &responseCompleteCallback }, & getObjectDataCallback };

static int read_obj(char * bucket_name, char * obj_name, char * buf, size_t obj_size){
  S3BucketContext * bucket = getBucket(bucket_name);
  struct data_handling dh = { .buf = buf, .size = obj_size };
  S3_get_object(bucket, obj_name, NULL, 0, obj_size, NULL, &getObjectHandler, & dh);
  CHECK_ERROR

  return MD_SUCCESS;
}

static S3Status statResponsePropertiesCallback(const S3ResponseProperties *properties, void *callbackData){
  // check the size
  size_t * obj_size = (size_t*) callbackData;
  if(*obj_size != properties->contentLength){
    //printf("%lu %lu\n",*obj_size, properties->contentLength);
     s3status = -1;
    return s3status;
  }
  s3status = S3StatusOK;
  return s3status;
}

static S3ResponseHandler statResponseHandler = {  &statResponsePropertiesCallback, &responseCompleteCallback };


static int stat_obj(char * bucket_name, char * obj_name, size_t obj_size){
  // how to ? Should use HEAD request, S3_head_object (?) or use S3_get_object with size = 1, offset = 0 ?
  S3BucketContext * bucket = getBucket(bucket_name);
  S3_head_object(bucket, obj_name, NULL, & statResponseHandler, & obj_size);
  CHECK_ERROR
  return MD_SUCCESS;
}

static int delete_obj(char * bucket_name, char * obj_name){
  S3BucketContext * bucket = getBucket(bucket_name);

  S3_delete_object(bucket, obj_name, NULL, & responseHandler, NULL);
  CHECK_ERROR
  return MD_SUCCESS;
}




struct md_plugin md_plugin_s3 = {
  "s3",
  get_options,
  initialize,
  finalize,
  prepare_global,
  purge_global,
  current_index,
  store_position,

  def_dset_name,
  create_dset,
  rm_dset,

  def_obj_name,
  write_obj,
  read_obj,
  stat_obj,
  delete_obj
};
