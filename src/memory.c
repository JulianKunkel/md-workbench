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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#include <md_util.h>

static uint64_t getValue(char * what){
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

  return (uint64_t) atoll(line);
}

static uint64_t getFreeRamKB(){
	return getValue("\nMemFree:") +getValue("\nCached:") + getValue("\nBuffers:");
}

int mem_preallocate(char ** allocP, uint64_t maxRAMinMB, int verbose){
  if(maxRAMinMB == 0){
    return 0;
  }
	uint64_t currentRAMinKB = getFreeRamKB();
  const uint32_t pagesize = getpagesize();

  assert(*allocP == NULL);
  const uint64_t maxRAMinKB = 1000 * maxRAMinMB;

  if(verbose){
	 printf ("starting to malloc RAM currently \n %lu KiB => goal %lu KiB\n", (uint64_t) currentRAMinKB, (uint64_t) maxRAMinKB);
 }

  uint64_t current_pos = 0;
  uint64_t end_pos;
	while(currentRAMinKB > maxRAMinKB){
		uint64_t delta = currentRAMinKB - maxRAMinKB;

		uint64_t toMalloc = (delta < (pagesize * 1000) ? delta + pagesize: pagesize * 1000);
    end_pos = current_pos + toMalloc;
    //printf("ALLOC %lu %lu %lu %lu\n", current_pos, end_pos, delta, currentRAMinKB);

		*allocP = realloc(*allocP, end_pos);
		if(*allocP == NULL){
			printf("could not allocate more RAM - retrying - free:%ld \n", (uint64_t) currentRAMinKB);
			return -1;
		}else{
      for(uint64_t p = current_pos; p < end_pos; p += pagesize){
        (*allocP)[p] = 1;
      }
		}
    current_pos += toMalloc;
		currentRAMinKB = getFreeRamKB();
	}

  if(verbose){
	 printf ("Finished now \n %lu - %lu\n", (uint64_t) currentRAMinKB, (uint64_t) maxRAMinKB);
  }

  return 0;
}

void mem_free_preallocated(char ** allocP){
  if(*allocP == NULL){
    return;
  }
  free(*allocP);
  *allocP = NULL;
}
