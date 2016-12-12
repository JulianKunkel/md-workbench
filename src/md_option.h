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

#ifndef MD_OPTION_H
#define MD_OPTION_H

typedef enum{
  OPTION_FLAG,
  OPTION_OPTIONAL_ARGUMENT,
  OPTION_REQUIRED_ARGUMENT
} option_value_type;

typedef struct{
  char shortVar;
  char * longVar;
  char * help;

  option_value_type arg;
  char type;  // data type, H = hidden string
  void * variable;
} option_help;

#define LAST_OPTION {0, 0, 0, (option_value_type) 0, 0, NULL}

void print_help(option_help * args, int is_plugin);

void print_current_options(option_help * args);

//@return the number of parsed arguments
int parseOptions(int argc, char ** argv, option_help * args, int * print_help);

#endif
