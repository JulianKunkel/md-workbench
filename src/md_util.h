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

#ifndef MD_UTIL_H
#define MD_UTIL_H

#include <stdint.h>
#include <time.h>

// timer functions
#ifdef ESM
typedef clock64_t timer;
#else
typedef struct timespec timer;
#endif

void start_timer(timer * t1);
double stop_timer(timer t1);

// allow to allocate memory
int mem_preallocate(char ** allocP, uint64_t maxRAMinMB, int verbose);
void mem_free_preallocated(char ** allocP);

#endif
