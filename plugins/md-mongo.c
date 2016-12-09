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

#include <bson.h>
#include <mongoc.h>

#include <plugins/md-mongo.h>

static char * database = "";
static char * username = "";
static char * password = "";
static char * host = "localhost";
static int port = 27017;

static char * global_coll_name = "md_real_io";
static mongoc_collection_t * global_collection = NULL; // global collection!
static int collection_per_dir = 0;
static int create_no_index = 0;

static option_help options [] = {
  {'D', "database", "Database name, this temporary database will be used for the test.", OPTION_REQUIRED_ARGUMENT, 's', & database},
  {'U', "user", "User name to access the database.", OPTION_OPTIONAL_ARGUMENT, 's', & username},
  {'H', "host", "Host name.", OPTION_OPTIONAL_ARGUMENT, 's', & host},
  {'p', "port", "Port.", OPTION_OPTIONAL_ARGUMENT, 'd', & port},
  {'P', "password", "Passwort, if empty no password is assumed.", OPTION_OPTIONAL_ARGUMENT, 's', & password},
  {'i', "no-index", "Create no index on the obj name.", OPTION_FLAG, 'd', & create_no_index},
  {'c', "use-collection-per-dir", "Create one collection per directory, otherwise a global collection is used", OPTION_FLAG, 'd', & collection_per_dir},
  LAST_OPTION
};

static option_help * get_options(){
  return options;
}

static mongoc_client_t * client;
static mongoc_database_t * mongo_db;

static int init_dir_internal(char * dir_name){
  // create a dummy collection and document to make sure the object can be created
  bson_t *doc;
  bson_error_t error;
  mongoc_collection_t * collection = mongoc_database_get_collection(mongo_db, dir_name);
  doc = bson_new ();
  bson_append_utf8(doc, "_id", 3, "empty", 5);

  if (! mongoc_collection_insert (collection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
    return MD_ERROR_UNKNOWN;
  }
  bson_destroy (doc);

  // create index by name
  if (! create_no_index){
    //doc = bson_new ();
    //bson_append_int32(doc, "obj", 3, 1);
    //ret = mongoc_collection_create_index (collection, doc, NULL, &error);
    //bson_destroy (doc);
    //if(! ret){
    //  printf("Error: %s\n", error.message);
    //}
  }
  mongoc_collection_destroy (collection);
  return MD_SUCCESS;
}

static int rm_dir_internal(char * dir_name){
  int ret;
  bson_t *doc;
  doc = bson_new ();
  mongoc_collection_t * collection = mongoc_database_get_collection(mongo_db, dir_name);
  mongoc_collection_remove(collection, MONGOC_REMOVE_NONE, doc, NULL, NULL );
  bson_destroy (doc);

  ret = mongoc_collection_drop(collection, NULL);
  mongoc_collection_destroy (collection);
  return ret ? MD_SUCCESS : MD_ERROR_UNKNOWN;
}

static int initialize(){
  mongoc_init ();
  char * conn_str = (char*) malloc(4096);
  char * current_pos = conn_str;

  int pos;
  pos = sprintf(conn_str, "mongodb://");
  current_pos += pos;
  if (strlen(username) > 1){
    pos = sprintf(current_pos, "%s", username);
    current_pos += pos;
  }
  if (strlen(password) > 1){
    pos = sprintf(current_pos, ":%s", password);
    current_pos += pos;
  }
  if (strlen(host) > 1){
    if(strlen(username) > 1) {
      pos = sprintf(current_pos, "@");
      current_pos += pos;
    }
    pos = sprintf(current_pos, "%s:%d", host, port);
    current_pos += pos;
  }

  pos = sprintf(current_pos, "/?authSource=%s&appname=md-real-io", database);

  client = mongoc_client_new (conn_str);
  //printf("%s\n", conn_str);
  if( ! conn_str ){
    printf("Cannot parse URI: %s\n", conn_str);
    return MD_ERROR_UNKNOWN;
  }
  free(conn_str);

  mongo_db = mongoc_client_get_database(client, database);
  const char * nam = mongoc_database_get_name(mongo_db);
  if (! mongo_db  || nam == NULL || strcmp(nam, database) != 0){
    return MD_ERROR_UNKNOWN;
  }

  if(! collection_per_dir){
    global_collection = mongoc_database_get_collection(mongo_db, global_coll_name);
  }
  return MD_SUCCESS;
}

static int finalize(){
  mongoc_database_destroy(mongo_db);
  mongoc_client_destroy (client);
  mongoc_cleanup ();

  if(! collection_per_dir){
    mongoc_collection_destroy (global_collection);
  }

  return MD_SUCCESS;
}

static int def_dset_name(char * out_name, int n, int d){
  if(collection_per_dir){
    sprintf(out_name, "%s_%d_%d", global_coll_name, n, d);
  }else{
    sprintf(out_name, "%s", global_coll_name);
  }
  return MD_SUCCESS;
}

static int def_obj_name(char * out_name, int n, int d, int i){
  if(collection_per_dir){
    sprintf(out_name, "%d", i);
  }else{
    sprintf(out_name, "%d_%d_%d", n, d, i);
  }
  return MD_SUCCESS;
}


static int prepare_global(){
  if(! collection_per_dir){
    int ret = init_dir_internal(global_coll_name);
    global_collection = mongoc_database_get_collection(mongo_db, global_coll_name);
    return ret;
  }
  // make sure we can actually write to the database
  return init_dir_internal("dummy");
}

static int purge_global(){
  if(! collection_per_dir){
    return rm_dir_internal(global_coll_name);
  }

  return rm_dir_internal("dummy");
}

static int create_dset(char * obj_name){
  if(collection_per_dir){
    return init_dir_internal(obj_name);
  }
  return MD_NOOP;
}

static int rm_dset(char * obj_name){
  if(collection_per_dir){
    return rm_dir_internal(obj_name);
  }
  return MD_NOOP;
}

static void construct_access(char * collname, char * obj_name, mongoc_collection_t ** out_collection, bson_t ** out_doc){
  if(collection_per_dir){
    *out_collection = mongoc_database_get_collection(mongo_db, collname);
  }else{
    *out_collection = global_collection;
  }
  bson_t *doc;

  doc = bson_new();
  //bson_oid_t oid;
  //bson_oid_init (&oid, NULL);
  //BSON_APPEND_OID(doc, "_id", &oid);
  if(create_no_index){
    bson_append_utf8(doc, "obj", 3, obj_name, strlen(obj_name));
  }else{
    bson_append_utf8(doc, "_id", 3, obj_name, strlen(obj_name));
  }

  *out_doc = doc;
}

static void free_access(mongoc_collection_t * collection, bson_t * doc){
  if(collection_per_dir){
    mongoc_collection_destroy (collection);
  }
  bson_destroy (doc);
}

static int write_obj(char * collname, char * obj_name, char * buf, size_t obj_size){
  int ret = MD_SUCCESS;
  bson_t *doc;
  bson_error_t error;
  mongoc_collection_t * collection;
  construct_access(collname, obj_name, & collection, & doc);

  if (! bson_append_binary (doc, "data", 4, BSON_SUBTYPE_BINARY, (const uint8_t*) buf, obj_size)){
    ret = MD_ERROR_UNKNOWN;
  }
  if (ret == MD_SUCCESS && ! mongoc_collection_insert (collection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
      ret = MD_ERROR_UNKNOWN;
  }
  free_access(collection, doc);
  return ret;
}


static int read_obj(char * collname, char * obj_name, char * buf, size_t obj_size){
  int ret = MD_SUCCESS;
  bson_t *doc;
  //bson_error_t error;
  mongoc_collection_t * collection;
  construct_access(collname, obj_name, & collection, & doc);

  mongoc_cursor_t * cursor = mongoc_collection_find_with_opts(collection, doc, NULL, NULL);

  const bson_t *element;
  if(mongoc_cursor_next (cursor, &element)) {
     bson_iter_t iter;
     bson_iter_init(& iter, element);
     while (bson_iter_next (&iter)) {
       if (strcmp(bson_iter_key (&iter), "data") == 0){
          const bson_value_t *value = bson_iter_value(&iter);
          if( value->value.v_binary.data_len != obj_size ){
            ret = MD_ERROR_UNKNOWN;
          }else{
            memcpy(buf, value->value.v_binary.data, obj_size);
            break;
          }
       }
     }
  }else{
    ret = MD_ERROR_FIND;
  }

  mongoc_cursor_destroy (cursor);
  free_access(collection, doc);
  return ret;
}

static int stat_obj(char * collname, char * obj_name, size_t obj_size){
  int ret = MD_SUCCESS;
  bson_t *doc;
  bson_error_t error;
  mongoc_collection_t * collection;
  construct_access(collname, obj_name, & collection, & doc);

  int64_t count = mongoc_collection_count (collection, MONGOC_QUERY_NONE, doc, 0,  0, NULL, & error );
  if (count < 0){
    printf("Error: %s\n", error.message);
    ret = MD_ERROR_UNKNOWN;
  }else if(count != 1){
    ret = MD_ERROR_FIND;
  }

  free_access(collection, doc);
  return ret;
}

static int delete_obj(char * collname, char * obj_name){
  bson_t *doc;
  bson_error_t error;
  mongoc_collection_t * collection;
  construct_access(collname, obj_name, & collection, & doc);

  mongoc_collection_remove(collection, MONGOC_REMOVE_NONE, doc, NULL, & error);
  free_access(collection, doc);
  return MD_SUCCESS;
}

struct md_plugin md_plugin_mongo = {
  "mongo",
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
