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

#include <time.h>

typedef struct timespec timer;

static void start_timer(timer * t1) {
    clock_gettime(CLOCK_MONOTONIC, t1);
}

static timer time_diff (struct timespec end, struct timespec start) {
    struct timespec diff;
    if (end.tv_nsec < start.tv_nsec) {
        diff.tv_sec = end.tv_sec - start.tv_sec - 1;
        diff.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
        diff.tv_sec = end.tv_sec - start.tv_sec;
        diff.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return diff;
}

static double time_to_double (struct timespec t) {
    double d = (double)t.tv_nsec;
    d /= 1000000000.0;
    d += (double)t.tv_sec;
    return d;
}

static double stop_timer(timer t1) {
    timer end;
    start_timer(& end);
    return time_to_double(time_diff(end, t1));
}
