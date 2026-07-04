#ifndef MSOCKET_H
#define MSOCKET_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <semaphore.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/shm.h>
#include <time.h>

#define SOCK_MTP 3
#define T 5

struct SOCK_INFO {
    int sock_id;
    int err_no;
    int port ;
    char ip[16];
};

typedef struct swnd {
        int seq_number[10];
        int window_size;
} swnd;

typedef struct rwnd {
        int seq_number[5];
        int window_size;
        int last_inorder_seq_no;    
} rwnd;

struct Socket {
    int free ;     // 0 if free else 1
    int pid;
    int sock_id;
    char destip[16];
    int destport;
    char sendbuf[10][100];
    time_t send_time[10];
    int sendbuf_size[10];   //if 0 then empty else filled
    int notyetack[10];     // 0 if not waiting for ack else 1
    char recvbuf[5][100]; 
    int recvbuf_size[5]; 
    swnd sender_window;
    rwnd receiver_window;
};


int m_socket(int domain, int type, int protocol);
int m_bind(int sockfd, char *srcip, int srcport, char *destip, int destport);
int m_sendto(int sockfd, const void *buf, size_t len, int flags, const struct sockaddr *dest_addr, socklen_t addrlen);
int m_recvfrom(int sockfd, void *buf, size_t len, int flags, struct sockaddr *src_addr, socklen_t *addrlen);

int dropMessage(float p);

void wait_sem(int sem_id);
void signal_sem(int sem_id);

#endif 