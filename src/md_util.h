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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#ifdef ESM
typedef clock64_t timer;

static void start_timer(timer * t1) {
    *t1 = clock64();
}

static double stop_timer(timer t1) {
    timer end;
    start_timer(& end);
    return (end - t1) / 1000.0 / 1000.0;
}


#else // POSIX COMPLAINT

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

static long getRAMValue(char * what){
        char buff[1024];

        int fd = open("/proc/meminfo", O_RDONLY);
        int ret = read(fd, buff, 1023);

        buff[ret>1023 ? 1023: ret] = 0;

        char * line = strstr(buff, what);

	if (line == 0){
		printf("Error %s not found in %s \n", what, buff);
		exit(1);
	}

	line += strlen(what) + 1;


        while(line[0] == ' '){
                line++;
        }

        int pos = 0;
        while(line[pos] != ' '){
                pos++;
        }
        line[pos] = 0;

        close(fd);

        return atoi(line);
}

#define GET_FREE_RAM getRAMValue("\nMemFree:") + getRAMValue("\nCached:") + getRAMValue("\nBuffers:")

void preallocate(long long int maxRAMinKB){
	long long int currentRAMinKB = GET_FREE_RAM;

	printf ("starting to malloc RAM currently \n %lld KiB => goal %lld KiB\n", currentRAMinKB, maxRAMinKB);

	while(currentRAMinKB > maxRAMinKB){
		long long int delta = currentRAMinKB - maxRAMinKB;
		long long int toMalloc = (delta < 500 ? delta : 500) * 1024;

		char * allocP = malloc(toMalloc);
		if(allocP == 0){
			printf("could not allocate more RAM - retrying - free:%lld \n", currentRAMinKB);
			sleep(5);
		}else{
			memset(allocP, '1', toMalloc);
		}
		currentRAMinKB = GET_FREE_RAM;
	}

	printf ("Finished now \n %lld - %lld\n", currentRAMinKB, maxRAMinKB);
}

#endif
