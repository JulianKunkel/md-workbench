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

/*
This plugin for md-real-io uses postgres to store virtually files and tbl_nameectories.
Directories are mapped to tables, if -t is not used, then one table is created and all files are stored with a unique file name in this table. (Flat scheme).
If -t is used, then for each tbl_nameectory, one table is created and the obj_name is a tuple in this table.

Anyway the schema is always:
(name VARCHAR PRIMARY KEY, data BYTEA)

Stat checks that the file size matches our expected file size
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <postgresql/libpq-fe.h>

#include <plugins/md-postgres.h>

static char * database = "";
static char * username = "";
static char * password = "";
static char * host = "";
static char * pq_options = "";
static char * tbl_name = "md_real_io";

static int table_per_tbl_name = 0;

static option_help options [] = {
  {'D', "database", "Database name, this temporary database will be used for the test.", OPTION_REQUIRED_ARGUMENT, 's', & database},
  {'U', "user", "User name to access the database.", OPTION_REQUIRED_ARGUMENT, 's', & username},
  {'H', "host", "Host name.", OPTION_OPTIONAL_ARGUMENT, 's', & host},
  {'O', NULL, "Postgres options (see conninfo)", OPTION_OPTIONAL_ARGUMENT, 's', & pq_options},
  {'T', "table-name", "Name of the table", OPTION_OPTIONAL_ARGUMENT, 's', & tbl_name},
  {'P', "password", "Passwort, if empty no password is assumed.", OPTION_OPTIONAL_ARGUMENT, 's', & password},
  {'t', "use-table-per-tbl_name", "Create one table per tbl_nameectory, otherwise a global table is used", OPTION_FLAG, 'd', & table_per_tbl_name},
  LAST_OPTION
};

static PGconn * conn = NULL;

static option_help * get_options(){
  return options;
}

static int prepare_global(){
  if( ! table_per_tbl_name ){
    char obj_name[1024];
    sprintf(obj_name, "CREATE TABLE %s(obj_name VARCHAR PRIMARY KEY, data BYTEA);", tbl_name);

    PGresult * res;
    res = PQexec(conn, obj_name);
    if (PQresultStatus(res) != PGRES_COMMAND_OK){
      printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
      PQclear(res);
      return MD_ERROR_UNKNOWN;
    }
    PQclear(res);
    return MD_SUCCESS;
  }

  return MD_NOOP;
}

static int purge_global(){
  if( ! table_per_tbl_name ){
    char obj_name[1024];
    sprintf(obj_name, "DROP TABLE %s;", tbl_name);

    PGresult * res;
    res = PQexec(conn, obj_name);
    if (PQresultStatus(res) != PGRES_COMMAND_OK){
      printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
      PQclear(res);
      return MD_ERROR_UNKNOWN;
    }
    PQclear(res);
    return MD_SUCCESS;
  }

  return MD_NOOP;
}

static int initialize(){
  if (conn != NULL) return MD_SUCCESS;
  char * connection_specifier = (char*) malloc(4096);
  char * current_pos = connection_specifier;

  int pos;
  pos = sprintf(connection_specifier, "dbname = %s", database);
  current_pos += pos;
  if (strlen(username) > 1){
    pos = sprintf(current_pos, " user = %s", username);
    current_pos += pos;
  }
  if (strlen(password) > 1){
    pos = sprintf(current_pos, " password = %s", password);
    current_pos += pos;
  }
  if (strlen(host) > 1){
    pos = sprintf(current_pos, " host = %s", host);
    current_pos += pos;
  }
  if (strlen(pq_options) > 1){
    pos = sprintf(current_pos, " options = %s", pq_options);
    current_pos += pos;
  }

  conn = PQconnectdb(connection_specifier);
  if (PQstatus(conn) != CONNECTION_OK){
      fprintf(stderr, "Connection to database failed: %s",  PQerrorMessage(conn));
      PQfinish(conn);
      conn = NULL;
      free(connection_specifier);
      return MD_ERROR_UNKNOWN;
  }
  free(connection_specifier);

  return MD_SUCCESS;
}

static int finalize(){
  PQfinish(conn);
  conn = NULL;
  return MD_SUCCESS;
}

static int def_dset_name(char * out_name, int n, int d){
  return MD_SUCCESS;
}


static int create_dset(char * obj_name, char * prefix, int rank, int iteration){
  if( table_per_tbl_name ){
    sprintf(obj_name, "CREATE TABLE %s_%d_%d(obj_name VARCHAR PRIMARY KEY, data BYTEA);", prefix, rank, iteration);
  }else{
    sprintf(obj_name, "INSERT INTO %s (obj_name, data) VALUES('%d/%d', NULL);", prefix, rank, iteration);
  }
  PGresult * res;
  res = PQexec(conn, obj_name);
  if (PQresultStatus(res) != PGRES_COMMAND_OK){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  PQclear(res);
  return MD_SUCCESS;
}

static int rm_dset(char * obj_name){
  if( table_per_tbl_name ){
    sprintf(obj_name, "DROP TABLE %s_%d_%d;", prefix, rank, iteration);
  }else{
    sprintf(obj_name, "DELETE FROM %s WHERE obj_name = '%d/%d';", prefix, rank, iteration);
  }
  PGresult * res;
  res = PQexec(conn, obj_name);
  if (PQresultStatus(res) != PGRES_COMMAND_OK){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  PQclear(res);
  return MD_SUCCESS;
}

static int def_obj_name(char * out_name, int n, int d, int i){
  sprintf(out_name, "%s/%d_%d/file-%d", dir, n, d, i);
  return MD_SUCCESS;
}


static int write_obj(char * obj_name, char * buf, size_t file_size){
  if( table_per_tbl_name ){
    sprintf(obj_name, "INSERT INTO %s_%d_%d(obj_name, data) VALUES('%d', $1::bytea)", prefix, rank, tbl_name, iteration);
  }else{
    sprintf(obj_name, "INSERT INTO %s (obj_name, data) VALUES('%d/%d/%d', $1::bytea)", prefix, rank, tbl_name, iteration);
  }
  PGresult * res;
  //printf("%s\n", obj_name);
  int paramFormats = 1;
  const int size = (int) file_size;
  res = PQexecParams(conn, obj_name, 1, NULL, (const char * const *) & buf, & size, & paramFormats, 1);
  if (PQresultStatus(res) != PGRES_COMMAND_OK){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  PQclear(res);
  return MD_SUCCESS;
}

static int read_obj(char * obj_name, char * buf, size_t file_size){
  if( table_per_tbl_name ){
    sprintf(obj_name, "SELECT data FROM %s_%d_%d WHERE obj_name = '%d';", prefix, rank, tbl_name, iteration);
  }else{
    sprintf(obj_name, "SELECT data FROM %s WHERE obj_name = '%d/%d/%d';", prefix, rank, tbl_name, iteration);
  }
  PGresult * res;
  res = PQexecParams(conn, obj_name, 0, NULL, NULL, NULL, NULL, 1);
  if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }

  int size = PQgetlength(res, 0, 0);
  const char* read_data = PQgetvalue(res, 0, 0);
  if (size > file_size){
    PQclear(res); // short read
    return MD_ERROR_UNKNOWN;
  }
  memcpy(buf, read_data, size);
  PQclear(res);

  if(file_size == size){
    return MD_SUCCESS;
  }

  return MD_ERROR_UNKNOWN;
}

static int stat_obj(char * obj_name, int file_size){
  PGresult * res;
  if( table_per_tbl_name ){
    sprintf(obj_name, "SELECT octet_length(data) FROM %s_%d_%d WHERE obj_name = '%d';", prefix, rank, tbl_name, iteration);
  }else{
    sprintf(obj_name, "SELECT octet_length(data) FROM %s WHERE obj_name = '%d/%d/%d';", prefix, rank, tbl_name, iteration);
  }
  res = PQexec(conn, obj_name);
  if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  // check the number of columns
  char * result = PQgetvalue(res, 0, 0);
  if (atoll(result) != file_size){
    PQclear(res);
    return MD_ERROR_FIND;
  }
  PQclear(res);
  return MD_SUCCESS;
}

static int delete_obj(char * obj_name){
  PGresult * res;
  if( table_per_tbl_name ){
    sprintf(obj_name, "DELETE FROM %s_%d_%d WHERE obj_name = '%d';", prefix, rank, tbl_name, iteration);
  }else{
    sprintf(obj_name, "DELETE FROM %s WHERE obj_name = '%d/%d/%d';", prefix, rank, tbl_name, iteration);
  }
  res = PQexec(conn, obj_name);
  if (strcmp(PQcmdTuples(res), "1") != 0){
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  if (PQresultStatus(res) != PGRES_COMMAND_OK){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  PQclear(res);
  return MD_SUCCESS;
}




struct md_plugin md_plugin_postgres = {
  "postgres",
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
