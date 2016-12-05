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

#include <plugins/md-posix.h>


static int prepare_testdir(char * dir){
  return mkdir(dir, 0755);
}

static int purge_testdir(char * dir){
  return rmdir(dir);
}


struct md_plugin md_plugin_posix = {
  "posix",
  prepare_testdir,
  purge_testdir,
};
