#include "msocket.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <semaphore.h>

#define MAX_SOCKETS 25

//A shared memory chunk SM containing the information about 25 MTP sockets

void wait_sem(int sem_id) {
    //printf("Init process waiting\n");

    struct sembuf sops;
    sops.sem_num = 0;
    sops.sem_op = -1;
    sops.sem_flg = 0;
    semop(sem_id, &sops, 1);
}

// Function to signal a semaphore
void signal_sem(int sem_id) {

    struct sembuf sops;
    sops.sem_num = 0;
    sops.sem_op = 1;
    sops.sem_flg = 0;   
    semop(sem_id, &sops, 1);
}

int m_socket(int domain, int type, int protocol)
{

    if (type != SOCK_MTP)
    {
        fprintf(stderr, "Invalid socket type\n");
        // errno = 1;
        exit(1);
    }
    

    int flag = 0;
    int index = 0;

    key_t key1;
    key1 = ftok(".",'a');

    int shmid1 = shmget(key1, MAX_SOCKETS*sizeof(struct Socket), IPC_CREAT | 0777);
    struct Socket *SM;
    SM = (struct Socket *)shmat(shmid1, NULL, 0);

    key_t key;
    key = ftok(".", 'b');
    // create a shared memory segment
    int shmid = shmget(key, sizeof(struct SOCK_INFO), IPC_CREAT | 0777);
    struct SOCK_INFO *Sinfo = (struct SOCK_INFO *)shmat(shmid, NULL, 0);


    for (index = 0; index < 25; index++)
    {
        if (SM[index].free == 0)
        {
            printf("%d\n", index);
            SM[index].free = 1;
            flag = 1;
            break;
        }
    }
    
    key_t semkeyA = ftok(".", 'A');
    int Sem1;
    Sem1 = semget(semkeyA, 1, IPC_CREAT | 0777);
    // Signal on Sem1
    signal_sem(Sem1);

    // Wait on Sem2
    key_t semkeyB = ftok(".", 'B');
    int Sem2;
    Sem2 = semget(semkeyB, 1, IPC_CREAT | 0777);

    wait_sem(Sem2);   
    if (flag == 0)
    {
        errno = ENOBUFS;
        return -1;
    }
    else
    {
        SM[index].sock_id = Sinfo->sock_id;
        SM[index].pid = getpid();
        return index;
    }

    
}

int m_bind(int sockfd, char *srcip, int srcport, char *destip, int destport)
{
    // running  a for loop to find the actual udp socket id from the SM Table
    key_t key1;
    key1 = ftok(".",'a');

    int shmid1 = shmget(key1, MAX_SOCKETS*sizeof(struct Socket), IPC_CREAT | 0777);
    struct Socket *SM;
    SM = (struct Socket *)shmat(shmid1, NULL, 0);
    

    // Put the UDP socket ID, IP, and port in SOCK_INFO table.
    key_t key;
    key = ftok(".", 'b');
    // create a shared memory segment
    int shmid = shmget(key, sizeof(struct SOCK_INFO), IPC_CREAT | 0777);
    struct SOCK_INFO *Sinfo = (struct SOCK_INFO *)shmat(shmid, NULL, 0);

    strcpy(Sinfo->ip, srcip);
    Sinfo->port = srcport;
    printf("%d %d\n", srcport, Sinfo->port);
    printf("%d\n", Sinfo->sock_id);

    key_t semkeyA = ftok(".", 'A');
    int Sem1;
    Sem1 = semget(semkeyA, 1, IPC_CREAT | 0777);
    key_t semkeyB = ftok(".", 'B');
    int Sem2;
    Sem2 = semget(semkeyB, 1, IPC_CREAT | 0777);

    // Signal on Sem1
    signal_sem(Sem1);

    // Wait on Sem2
    wait_sem(Sem2);

    if (Sinfo->sock_id == -1)
    {
        errno = Sinfo->err_no;
        Sinfo->sock_id = 0;
        memset(Sinfo->ip, 0, sizeof(Sinfo->ip));
        Sinfo->port = 0;
        Sinfo->err_no = 0;
        return -1;
    }
    else
    {
        Sinfo->sock_id = 0;
        memset(Sinfo->ip, 0, sizeof(Sinfo->ip));
        Sinfo->port = 0;
        Sinfo->err_no = 0;

        
        strcpy(SM[sockfd].destip, destip);
        SM[sockfd].destport = destport;
        return 0;   
    }

}

int m_sendto(int sockfd, const void *buf, size_t len, int flags,const struct sockaddr *dest_addr, socklen_t dest_len)
{
    
    int index = sockfd;
    key_t key1;
    key1 = ftok(".",'a');
    int shmid1 = shmget(key1, MAX_SOCKETS*sizeof(struct Socket), IPC_CREAT | 0777);
    struct Socket *SM;
    SM = (struct Socket *)shmat(shmid1, NULL, 0);
    
    struct sockaddr_in *dest = (struct sockaddr_in *)dest_addr;
    
    char *temp = (char *)malloc(17 * sizeof(char));
    strcpy(temp, SM[index].destip);

    if(strcmp(temp,inet_ntoa((dest->sin_addr))) || SM[index].destport != ntohs(dest->sin_port))
    {
        errno = ENOTCONN;          
        return -1;
    }

    //check if there is space in the send buffer
    int i = 0;
    char *buffer = (char *)buf;

    for (i = 0; i < 10; i++)
    {
        if (strcmp(SM[index].sendbuf[i], "") == 0)
        {     
            strcpy(SM[index].sendbuf[i], buffer);
            break;
        }
    }
    if (i == 10)
    {
        errno = ENOBUFS;
        return -1;
    }
    printf("%d\n", strlen(SM[index].sendbuf[i]));
    return strlen(SM[index].sendbuf[i]);
}



int m_recvfrom(int sockfd, void  *restrict buf, size_t len, int flags, struct sockaddr *restrict address, socklen_t *restrict address_len)
{
    
    int index = sockfd;
    key_t key1;
    key1 = ftok(".",'a');
    int shmid1 = shmget(key1, MAX_SOCKETS*sizeof(struct Socket), IPC_CREAT | 0777);
    struct Socket *SM = (struct Socket *)shmat(shmid1, NULL, 0);
    
    // printf("m_recvfrom called\n");
    int i;
    char * buffer = (char *)buf;
    for (i = 0; i < 5; i++)        
    {
        // printf("%s\n", SM[index].recvbuf[i]);
        //printf("String = %s", SM[index].recvbuf[i]);
        // printf("recvbuf[%d] = %s\n", i, SM[index].recvbuf[i]);
        if (strcmp(SM[index].recvbuf[i], "") != 0)
        {
            strcpy((char*)buf, SM[index].recvbuf[i]);
            //printf("%s\n", (char*)buf); 
            memset(SM[index].recvbuf[i], 0, sizeof(SM[index].recvbuf[i]));
            break;
        }
    }
   // printf("bjjgg\n");
    //printf("%s\n", buffer);
    if (i == 5)
    {
        errno = ENOMSG;
        return -1;
    }
    return strlen(buf);
}

int dropMessage(float p)
{
    srand(time(NULL));
    float r = (float)rand() / (float)RAND_MAX;
    if (r < p)
    {
        return 1;
    }
    return 0;
}

