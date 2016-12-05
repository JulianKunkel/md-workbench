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
This plugin for md-real-io uses postgres to store virtually files and directories.
Directories are mapped to tables, if -t is not used, then one table is created and all files are stored with a unique file name in this table. (Flat scheme).
If -t is used, then for each directory, one table is created and the filename is a tuple in this table.

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

static int table_per_dir = 0;

static option_help options [] = {
  {'D', "database", "Database name, this temporary database will be used for the test.", OPTION_REQUIRED_ARGUMENT, 's', & database},
  {'U', "user", "User name to access the database.", OPTION_REQUIRED_ARGUMENT, 's', & username},
  {'H', "host", "Host name.", OPTION_OPTIONAL_ARGUMENT, 's', & host},
  {'O', NULL, "Postgres options (see conninfo)", OPTION_OPTIONAL_ARGUMENT, 's', & pq_options},
  {'P', "password", "Passwort, if empty no password is assumed.", OPTION_OPTIONAL_ARGUMENT, 's', & password},
  {'t', "use-table-per-dir", "Create one table per directory, otherwise a global table is used", OPTION_FLAG, 'd', & table_per_dir},
  LAST_OPTION
};

static PGconn * conn = NULL;

static option_help * get_options(){
  return options;
}

static int prepare_testdir(char * dir){
  if( ! table_per_dir ){
    char filename[1024];
    sprintf(filename, "CREATE TABLE %s(filename VARCHAR PRIMARY KEY, data BYTEA);", dir);

    PGresult * res;
    res = PQexec(conn, filename);
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

static int purge_testdir(char * dir){
  if( ! table_per_dir ){
    char filename[1024];
    sprintf(filename, "DROP TABLE %s;", dir);

    PGresult * res;
    res = PQexec(conn, filename);
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


static int create_rank_dir(char * filename, char * prefix, int rank){
  return MD_NOOP;
}

static int rm_rank_dir(char * filename, char * prefix, int rank){
  return MD_NOOP;
}


static int create_dir(char * filename, char * prefix, int rank, int iteration){
  if( table_per_dir ){
    sprintf(filename, "CREATE TABLE %s_%d_%d(filename VARCHAR PRIMARY KEY, data BYTEA);", prefix, rank, iteration);
  }else{
    sprintf(filename, "INSERT INTO %s (filename, data) VALUES('%d/%d', NULL);", prefix, rank, iteration);
  }
  PGresult * res;
  res = PQexec(conn, filename);
  if (PQresultStatus(res) != PGRES_COMMAND_OK){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  PQclear(res);
  return MD_SUCCESS;
}

static int rm_dir(char * filename, char * prefix, int rank, int iteration){
  if( table_per_dir ){
    sprintf(filename, "DROP TABLE %s_%d_%d;", prefix, rank, iteration);
  }else{
    sprintf(filename, "DELETE FROM %s WHERE filename = '%d/%d';", prefix, rank, iteration);
  }
  PGresult * res;
  res = PQexec(conn, filename);
  if (PQresultStatus(res) != PGRES_COMMAND_OK){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  PQclear(res);
  return MD_SUCCESS;
}

static int write_file(char * filename, char * buf, size_t file_size,  char * prefix, int rank, int dir, int iteration){
  if( table_per_dir ){
    sprintf(filename, "INSERT INTO %s_%d_%d(filename, data) VALUES('%d', $1::bytea)", prefix, rank, dir, iteration);
  }else{
    sprintf(filename, "INSERT INTO %s (filename, data) VALUES('%d/%d/%d', $1::bytea)", prefix, rank, dir, iteration);
  }
  PGresult * res;
  //printf("%s\n", filename);
  int paramFormats = 1;
  const int size = (int) file_size;
  res = PQexecParams(conn, filename, 1, NULL, (const char * const *) & buf, & size, & paramFormats, 1);
  if (PQresultStatus(res) != PGRES_COMMAND_OK){
    printf("PSQL error (%s): %s - Connection: %s\n", PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res), PQerrorMessage(conn));
    PQclear(res);
    return MD_ERROR_UNKNOWN;
  }
  PQclear(res);
  return MD_SUCCESS;
}

static int read_file(char * filename, char * buf, size_t file_size,  char * prefix, int rank, int dir, int iteration){
  if( table_per_dir ){
    sprintf(filename, "SELECT data FROM %s_%d_%d WHERE filename = '%d';", prefix, rank, dir, iteration);
  }else{
    sprintf(filename, "SELECT data FROM %s WHERE filename = '%d/%d/%d';", prefix, rank, dir, iteration);
  }
  PGresult * res;
  res = PQexecParams(conn, filename, 0, NULL, NULL, NULL, NULL, 1);
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

static int stat_file(char * filename, char * prefix, int rank, int dir, int iteration, int file_size){
  PGresult * res;
  if( table_per_dir ){
    sprintf(filename, "SELECT octet_length(data) FROM %s_%d_%d WHERE filename = '%d';", prefix, rank, dir, iteration);
  }else{
    sprintf(filename, "SELECT octet_length(data) FROM %s WHERE filename = '%d/%d/%d';", prefix, rank, dir, iteration);
  }
  res = PQexec(conn, filename);
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

static int delete_file(char * filename, char * prefix, int rank, int dir, int iteration){
  PGresult * res;
  if( table_per_dir ){
    sprintf(filename, "DELETE FROM %s_%d_%d WHERE filename = '%d';", prefix, rank, dir, iteration);
  }else{
    sprintf(filename, "DELETE FROM %s WHERE filename = '%d/%d/%d';", prefix, rank, dir, iteration);
  }
  res = PQexec(conn, filename);
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
