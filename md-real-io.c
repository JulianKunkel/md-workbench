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

#include <mpi.h>

#include <stdio.h>

#include "util.h"
#include "option.h"

char * dir = "./out";

int main(int argc, char ** argv){
  option_help options [] = {
    {'d', "directory", "Directory where to run the benchmark.", OPTION_REQUIRED_ARGUMENT, 's', & dir},
    {0, 0, 0, 0, 0, NULL}
    };
  parseOptions(argc, argv, options);

  timer bench_start;
  start_timer(& bench_start);
  double runtime = stop_timer(bench_start);
  printf("Runtime: %.2fs\n", runtime);
  return 0;
}
