#include "../include/shared_memory_lab.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>

// do not need to modify create_segment
int create_segment(void) {
    return shmget(IPC_PRIVATE, sizeof(struct shared_block), 0600 | IPC_CREAT);
    // when attach needs to be private
    // IPC Private need to be create and make private
    // use as private memory so need to generate unique token
}

void cleanup_segment(int segment_id, struct shared_block *block) {
    if (shmdt(block) == -1) {
        perror("shmdt");
    }
    if (shmctl(segment_id, IPC_RMID, NULL) == -1) {
        perror("shmctl");
    }
}

static void wait_for_ready(const struct shared_block *block) {
    while (!block->ready) {
        usleep(1000);
    }
}

/* Handshake states for block->ready used by the chunked transfer in Task A. */
#define CHUNK_EMPTY 0 /* buffer free: parent may write the next chunk   */
#define CHUNK_FULL  1 /* chunk written: child may read it               */
#define CHUNK_DONE  2 /* no more chunks: child stops reading            */

static void wait_until_consumed(const struct shared_block *block) {
    while (block->ready != CHUNK_EMPTY) {
        usleep(1000);
    }
}

int task_fast_exchange(void) // it will fork and create a child process
{
    int segment_id;
    pid_t child_pid;
    int status;
    struct shared_block *block;

    // create shared memory
    segment_id = create_segment();
    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);
    if (block == (void *)-1) {
        perror("shmat");
        return 1;
    }
    memset(block, 0, sizeof(*block));

    child_pid = fork();
    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (child_pid == 0) {
        /* Child: the 21-char message is larger than the 16-byte buffer, so it
         * arrives in chunks. Reassemble them until the parent signals DONE. */
        char received[64] = "";
        size_t received_len = 0;

        for (;;) {
            wait_for_ready(block);
            __sync_synchronize(); /* see the parent's message bytes */
            if (block->ready == CHUNK_DONE) {
                break;
            }
            size_t n = strlen(block->message);
            if (received_len + n >= sizeof(received)) {
                fprintf(stderr, "child: receive buffer overflow\n");
                _exit(1);
            }
            memcpy(received + received_len, block->message, n);
            received_len += n;
            received[received_len] = '\0';
            __sync_synchronize();
            block->ready = CHUNK_EMPTY; /* hand the buffer back */
        }

        printf("child fast read: %s\n", received);
        fflush(stdout);
        /* Detach, then _exit so the child never returns into main(). */
        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }
        _exit(0);
    }

    /* Parent: write the message directly into shared memory, at most
     * SHM_MESSAGE_CAP - 1 bytes at a time, waiting for the child to take
     * each chunk before overwriting the buffer. */
    {
        const char *text = "shared memory is fast";
        size_t total = strlen(text);
        size_t offset = 0;

        while (offset < total) {
            size_t n = total - offset;
            if (n > SHM_MESSAGE_CAP - 1) {
                n = SHM_MESSAGE_CAP - 1;
            }
            memcpy(block->message, text + offset, n);
            block->message[n] = '\0';
            __sync_synchronize(); /* message bytes before the flag */
            block->ready = CHUNK_FULL;
            wait_until_consumed(block);
            __sync_synchronize();
            offset += n;
        }
        block->ready = CHUNK_DONE;
    }

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, block);
        return 1;
    }

    printf("parent fast write: done\n");
    fflush(stdout);

    cleanup_segment(segment_id, block);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}

int task_latest_value_limit(void) {
    int segment_id;
    pid_t child_pid;
    int status;
    struct shared_block *block;

    segment_id = create_segment();
    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);
    if (block == (void *)-1) {
        perror("shmat");
        return 1;
    }
    memset(block, 0, sizeof(*block));

    child_pid = fork();
    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (child_pid == 0) {
        wait_for_ready(block);
        printf("child latest view: %s\n", block->message);
        fflush(stdout);
        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }
        _exit(0);
    }

    /* Parent: two writes to the SAME bytes. The second overwrites the first,
     * so the child can only ever observe "latest-only". */
    snprintf(block->message, sizeof(block->message), "%s", "first update");
    snprintf(block->message, sizeof(block->message), "%s", "latest-only");
    block->ready = 1;

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, block);
        return 1;
    }

    printf("limit observed: no history in shared memory\n");
    fflush(stdout);

    cleanup_segment(segment_id, block);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}

int task_capacity_limit(void) {
    int segment_id;
    pid_t child_pid;
    int status;
    struct shared_block *block;
    const char *source = "abcdefghijklmnopqrstuvwxyz";
    int source_len;

    segment_id = create_segment();
    if (segment_id == -1) {
        perror("shmget");
        return 1;
    }

    block = shmat(segment_id, NULL, 0);
    if (block == (void *)-1) {
        perror("shmat");
        return 1;
    }
    memset(block, 0, sizeof(*block));

    child_pid = fork();
    if (child_pid < 0) {
        perror("fork");
        cleanup_segment(segment_id, block);
        return 1;
    }

    if (child_pid == 0) 
    // almost the same as the previous task except this part
    {
        wait_for_ready(block);
        /* Only SHM_MESSAGE_CAP - 1 = 15 chars fit; the 16th byte is '\0'. */
        printf("child capacity bytes: %zu\n", strlen(block->message));
        fflush(stdout);
        if (shmdt(block) == -1) {
            perror("shmdt");
            _exit(1);
        }
        _exit(0);
    }

    /* snprintf never writes past the buffer; its return value is the length
     * it WANTED to write (26), which shows how much data did not fit. */
    source_len = snprintf(block->message, sizeof(block->message), "%s", source);
    block->ready = 1;

    if (waitpid(child_pid, &status, 0) == -1) {
        perror("waitpid");
        cleanup_segment(segment_id, block);
        return 1;
    }

    printf("parent source bytes: %d\n", source_len);
    fflush(stdout);

    cleanup_segment(segment_id, block);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}

int main(void) {
    if (task_fast_exchange() != 0) {
        return 1;
    }

    if (task_latest_value_limit() != 0) {
        return 1;
    }

    if (task_capacity_limit() != 0) {
        return 1;
    }

    printf("all tasks done\n");
    return 0;
}