/* SPDX-License-Identifier: MIT */
/* Copyright (c) 2026 Balaje Sankar */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>
#include <termios.h>
#include <err.h>
#include <libutil.h>
#include <fcntl.h>
#include <stddef.h>
struct sockaddr_hvs {
    unsigned char sa_len;
    sa_family_t sa_family;
    unsigned int hvs_port;
    unsigned char hvs_zero[sizeof(struct sockaddr) -
                           sizeof(sa_family_t) -
                           sizeof(unsigned char) -
                           sizeof(unsigned int)];
};


struct MESSAGE_HEADER {
    unsigned int MessageType;
    unsigned int MessageSize;
    unsigned int SequenceNumber;
};


typedef struct _LX_INIT_CREATE_PROCESS_COMMON
{
    unsigned int FilenameOffset;
    unsigned int CurrentWorkingDirectoryOffset;
    unsigned int CommandLineOffset;
    unsigned short CommandLineCount;
    unsigned int EnvironmentOffset;
    unsigned short EnvironmentCount;
    unsigned int NtEnvironmentOffset;
    unsigned short NtEnvironmentCount;
    unsigned int NtPathOffset;
    unsigned int ShellOptions;
    unsigned int UsernameOffset;
    unsigned int DefaultUid;
    int Flags;
    char Buffer[]; // flexible array member
} LX_INIT_CREATE_PROCESS_COMMON;

typedef struct _LX_INIT_CREATE_PROCESS_UTILITY_VM
{
    struct MESSAGE_HEADER Header;
    unsigned short Rows;
    unsigned short Columns;
    LX_INIT_CREATE_PROCESS_COMMON Common;
} LX_INIT_CREATE_PROCESS_UTILITY_VM;

typedef struct RESULT_MESSAGE_U32
{
    struct MESSAGE_HEADER Header;
    uint32_t Result;
} RESULT_MESSAGE_U32;

// Global storage for initial message
LX_INIT_CREATE_PROCESS_UTILITY_VM *g_initial_message = NULL;
size_t g_initial_message_size = 0;

#define NUM_ADDITIONAL_SOCKETS 5
#define ACCEPT_TIMEOUT_MS 60000 // 60s

static ssize_t read_full(int fd, void *buf, size_t count) {
    size_t total = 0;
    char *p = buf;
    while (total < count) {
        ssize_t r = read(fd, p + total, count - total);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return total; // EOF
        total += (size_t)r;
    }
    return (ssize_t)total;
}

int receive_initial_message(int sock_fd)
{
    // Step 1: read fixed-size message header
    struct MESSAGE_HEADER header;
    ssize_t r = read_full(sock_fd, &header, sizeof(header));
    if (r <= 0) {
        if (r == 0) fprintf(stderr, "peer closed while reading header\n");
        else perror("read header");
        return -1;
    }

    // Validate header.MessageSize
    if (header.MessageSize < sizeof(header)) {
        fprintf(stderr, "invalid message size: %u\n", header.MessageSize);
        return -1;
    }

    // Allocate buffer for entire message
    size_t msg_size = (size_t)header.MessageSize;
    LX_INIT_CREATE_PROCESS_UTILITY_VM *msg = malloc(msg_size);
    if (!msg) {
        perror("malloc");
        return -1;
    }

    // Copy header we already read
    memcpy(&msg->Header, &header, sizeof(header));

    // Read the remaining bytes (msg_size - sizeof(header))
    ssize_t rem = read_full(sock_fd, ((char *)msg) + sizeof(header), msg_size - sizeof(header));
    if (rem < 0) {
        perror("read message body");
        free(msg);
        return -1;
    }
    if ((size_t)rem != msg_size - sizeof(header)) {
        fprintf(stderr, "short read: expected %zu, got %zd\n", msg_size - sizeof(header), rem);
        free(msg);
        return -1;
    }

    // Store in global structure for interop use
    if (g_initial_message)
        free(g_initial_message);
    g_initial_message = msg;
    g_initial_message_size = msg_size;

    // For debugging, safely access Rows/Columns only if the message is large enough
    if (g_initial_message_size >= offsetof(LX_INIT_CREATE_PROCESS_UTILITY_VM, Common)) {
        printf("Stored initial message: Rows=%u, Columns=%u\n",
               (unsigned int)g_initial_message->Rows,
               (unsigned int)g_initial_message->Columns);
    } else {
        printf("Stored initial message (size %zu) but cannot read Rows/Columns safely.\n", g_initial_message_size);
    }

    RESULT_MESSAGE_U32 resp;
    memset(&resp, 0, sizeof(resp));
    // Reply using the original message's type/sequence (echo)
    resp.Header.MessageType = 78;
    resp.Header.MessageSize = sizeof(resp);
    resp.Header.SequenceNumber = 1;
    resp.Result = 60000; // success

    // Step 3: send back
    ssize_t w = write(sock_fd, &resp, sizeof(resp));
    if (w != sizeof(resp)){
        perror("write result message");
        return -1;
    }

    return 0;
}

void handle_interop_channel(int sock_fd)
{
    if (!g_initial_message) {
        fprintf(stderr, "No initial message stored!\n");
        close(sock_fd);
        return;
    }

    // Make a copy so we don't mutate the stored message unexpectedly
    LX_INIT_CREATE_PROCESS_UTILITY_VM *to_send = malloc(g_initial_message_size);
    if (!to_send) {
        perror("malloc");
        close(sock_fd);
        return;
    }
    memcpy(to_send, g_initial_message, g_initial_message_size);

    // Modify fields intentionally for interop (as original code did)
    to_send->Header.MessageType = 8; // per protocol
    to_send->Header.SequenceNumber = 1;
    to_send->Header.MessageSize = (unsigned int)g_initial_message_size;

    // Send the stored initial message
    ssize_t w = write(sock_fd, to_send, g_initial_message_size);
    if (w != (ssize_t)g_initial_message_size) {
        perror("write interop message");
        free(to_send);
        close(sock_fd);
        return;
    }

    free(to_send);

    // Optionally echo back any further data
    char buf[4096];
    for (;;) {
        ssize_t r = read(sock_fd, buf, sizeof(buf));
        if (r <= 0) break;
        write(sock_fd, buf, r);
    }

    close(sock_fd);
}


int handle_console(int stdin_fd, int stdout_fd, int stderr_fd) {
    int master_fd;
    pid_t pid;

    struct winsize ws = {24, 80, 0, 0}; // default terminal size

    // Fork a new pseudo-terminal
    pid = forkpty(&master_fd, NULL, NULL, &ws);
    if (pid < 0) {
        perror("forkpty");
        return -1;
    }

    if (pid == 0) {
        // CHILD PROCESS
        // The child is already connected to the slave side of the PTY
        // No need to set raw mode or dup2 anything - forkpty handles this

        // Execute the FreeBSD default shell
        execlp("/bin/sh", "sh", (char*)NULL);
        perror("execlp");
        _exit(127);
    }

    // PARENT PROCESS
    // Make master_fd non-blocking to prevent hanging
    int flags = fcntl(master_fd, F_GETFL);
    if (flags >= 0) {
        fcntl(master_fd, F_SETFL, flags | O_NONBLOCK);
    }

    // Also make stdin_fd non-blocking
    flags = fcntl(stdin_fd, F_GETFL);
    if (flags >= 0) {
        fcntl(stdin_fd, F_SETFL, flags | O_NONBLOCK);
    }

    // Relay loop: forward bytes between master_fd and stdin/stdout pipes
    fd_set readfds;
    char buf[4096];
    int n;
    struct timeval timeout;

    while (1) {
        // Check if child process is still alive
        int status;
        pid_t result = waitpid(pid, &status, WNOHANG);
        if (result == pid) {
            // Child has exited
            break;
        } else if (result < 0 && errno != ECHILD) {
            perror("waitpid");
            break;
        }

        FD_ZERO(&readfds);
        FD_SET(master_fd, &readfds);
        FD_SET(stdin_fd, &readfds);

        int nfds = (master_fd > stdin_fd ? master_fd : stdin_fd) + 1;

        // Add timeout to prevent indefinite blocking
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int ret = select(nfds, &readfds, NULL, NULL, &timeout);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }
        if (ret == 0) {
            // Timeout - continue loop to check child status
            continue;
        }

        // From PTY -> stdout
        if (FD_ISSET(master_fd, &readfds)) {
            n = read(master_fd, buf, sizeof(buf));
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No data available right now
                    continue;
                }
                perror("read from master_fd");
                break;
            }
            if (n == 0) {
                // PTY closed
                break;
            }

            ssize_t written = 0;
            while (written < n) {
                ssize_t w = write(stdout_fd, buf + written, n - written);
                if (w < 0) {
                    if (errno == EINTR) continue;
                    perror("write to stdout_fd");
                    goto cleanup;
                }
                written += w;
            }
        }

        // From stdin -> PTY
        if (FD_ISSET(stdin_fd, &readfds)) {
            n = read(stdin_fd, buf, sizeof(buf));
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No data available right now
                    continue;
                }
                perror("read from stdin_fd");
                break;
            }
            if (n == 0) {
                // stdin closed
                break;
            }

            ssize_t written = 0;
            while (written < n) {
                ssize_t w = write(master_fd, buf + written, n - written);
                if (w < 0) {
                    if (errno == EINTR) continue;
                    perror("write to master_fd");
                    goto cleanup;
                }
                written += w;
            }
        }
    }

cleanup:
    // Clean up
    close(master_fd);

    // Wait for child to exit (if not already)
    int status;
    waitpid(pid, &status, 0);

    return WEXITSTATUS(status);
}


/* Communication / interop channel */
void handle_channel(int sock_fd) {
    char buf[4096];
    for (;;) {
        ssize_t r = read(sock_fd, buf, sizeof(buf));
        if (r <= 0) break;
        write(sock_fd, buf, r); // echo back
    }
    close(sock_fd);
}

/* Accept socket with poll timeout */
int accept_with_poll(int listen_fd) {
    struct pollfd pfd;
    pfd.fd = listen_fd;
    pfd.events = POLLIN;

    int ret = poll(&pfd, 1, ACCEPT_TIMEOUT_MS);
    if (ret <= 0) {
        if (ret == 0)
            fprintf(stderr, "accept timeout\n");
        else
            perror("poll");
        return -1;
    }

    int client_fd = accept(listen_fd, NULL, NULL);
    if (client_fd < 0) {
        perror("accept");
        return -1;
    }

    return client_fd;
}

int main(void) {
    int s;
    struct sockaddr_hvs addr;

    s = socket(AF_HYPERV, SOCK_STREAM, 0);
    if (s < 0) {
        perror("socket");
        exit(1);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sa_len = sizeof(addr);
    addr.sa_family = AF_HYPERV;
    addr.hvs_port = 60000;

    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(s);
        exit(1);
    }

    if (listen(s, 10) < 0) { // bigger backlog
        perror("listen");
        close(s);
        exit(1);
    }

    printf("hv_sock server listening on port %u...\n", addr.hvs_port);
    /* Accept first connection to init */
    int init_c = accept_with_poll(s);
    if(init_c < 0) {
       fprintf(stderr, "Failed first connectiion to init");
       close(s);
       exit(1);
     }
    /* Accept initial connection */
    int initial_c = accept_with_poll(s);
    if (initial_c < 0) {
        fprintf(stderr, "Failed initial connection\n");
        close(s);
        exit(1);
    }
    printf("Initial connection accepted.\n");
    if (receive_initial_message(initial_c) < 0) {
        close(initial_c);
        close(s);
        exit(1);
    }

    /* Accept 5 additional sockets */
    int client_sockets[NUM_ADDITIONAL_SOCKETS];
    for (int i = 0; i < NUM_ADDITIONAL_SOCKETS; ++i) {
        client_sockets[i] = accept_with_poll(s);
        if (client_sockets[i] < 0) {
            fprintf(stderr, "Failed additional connection %d\n", i);
            close(s);
            exit(1);
        }
        printf("Additional connection %d accepted.\n", i);
    }

    /* PTY console (stdin/out/err) */
    if (fork() == 0) {
        handle_console(client_sockets[0], client_sockets[1], client_sockets[2]);
        _exit(0);
    } else {
        close(client_sockets[0]);
        close(client_sockets[1]);
        close(client_sockets[2]);
    }

    /* Communication / interop */

    for (int i = 3; i < NUM_ADDITIONAL_SOCKETS; ++i) {
    if (fork() == 0) {
        if (i == NUM_ADDITIONAL_SOCKETS - 1) {
            /* last socket = interop socket */
            handle_interop_channel(client_sockets[i]);
        } else {
            handle_channel(client_sockets[i]);
        }
        _exit(0);
    } else {
        close(client_sockets[i]);
    }
}

    /* Initial connection */
    if (fork() == 0) {
        handle_channel(initial_c);
        _exit(0);
    } else {
        close(initial_c);
    }

    /* Wait for all child processes */
    for (int i = 0; i < NUM_ADDITIONAL_SOCKETS + 1; ++i)
        wait(NULL);

    close(s);
    return 0;
}
