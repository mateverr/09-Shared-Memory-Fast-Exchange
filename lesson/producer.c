// producer.c -> write to shared mem
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <unistd.h>

#define MEM_SIZE 4096

struct shm_st {
  int written;
  char data[BUFSIZ];
};

int main() {
  int running = 1, shmID;
  void *sh_mem = NULL;
  struct shm_st *sh_area;

  char buffer[BUFSIZ];
  key_t key;

  // Generate a unique key
  // key = ftok("/tmp", 'P');
  // if (key == -1) {
  //     perror("ftok");
  //     exit(EXIT_FAILURE);
  // }

  // shmID = shmget(key, MEM_SIZE, 0666 | IPC_CREAT);  // get shmID with size and perm or with IPC_CREAT (this line is for ftok)
  shmID = shmget((key_t) 1234, MEM_SIZE, 0666 | IPC_CREAT);  // get shmID
  // key is 1234
  // 0666 grant access to every user in the system
  if (shmID == -1) 
  {
    fprintf(stderr, "shmget failed\n");
    exit(EXIT_FAILURE);
  }

  sh_mem = shmat(shmID, NULL, 0);  // attach shared memory to the process, null means system will figure out available location
  if (sh_mem == (void *) -1) // -1 if failure exit, and print the address
  {
    fprintf(stderr, "shmat failed\n");
    exit(EXIT_FAILURE);
  }
  printf("Memory attached at %p\n", sh_mem); //assign data
  sh_area = (struct shm_st *)sh_mem;  // type casting, to provide exact type to sh_mem, copy address from sh_area to sh_mem
  // points to the same address/location
  sh_area->written=0;

  while (running) 
  {
    while (sh_area->written) // need a delay as shm is not synchronized
    // points to the first element of the structure
    // if written = 0, exit loop and prompt for input
    {
      // if 1 keep sleeping
      sleep(1); 
      printf("Waiting...\n");
    }
    printf("Enter data : ");
    fgets(buffer, BUFSIZ, stdin);
    strcpy(sh_area->data, buffer); // once get data, writes data to shared memory
    sh_area->written = 1; // once write to shared memory, change status to 1, to awaits new input

    if (strncmp(buffer, "end", 3) == 0) {running = 0;}
    // if end, get out of the loop
  }

  if (shmdt(sh_mem) == -1) // detach
  {
    fprintf(stderr, "shmdt failed\n");
    exit(EXIT_FAILURE);
  }
  exit(EXIT_SUCCESS);
}