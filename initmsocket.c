#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <semaphore.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <pthread.h>
#include <sys/select.h>
#include <time.h>
#include "msocket.h"

int sem_mtx;

#define MAX_SOCKETS 25

int isTimeout(time_t start, time_t end)
{
    // printf("start time:%d and end time: %d", start, end);
    if (end - start >= 5)
    {
        return 1;
    }
    return 0;
}

void *R(void *arg)
{
    // Get the shared memory segment containing socket information
    key_t key = ftok(".", 'b');
    int shmid = shmget(key, sizeof(struct SOCK_INFO), 0777 | IPC_CREAT);
    if (shmid == -1)
    {
        perror("shmget failed");
        exit(1);
    }

    struct SOCK_INFO *Sinfo = (struct SOCK_INFO *)shmat(shmid, NULL, 0);
    if (Sinfo == (struct SOCK_INFO *)-1)
    {
        perror("shmat failed");
        exit(1);
    }

    struct Socket *SM;
    key_t key1;
    key1 = ftok(".", 'a');

    int shmid1 = shmget(key1, MAX_SOCKETS * sizeof(struct Socket), IPC_CREAT | 0777);
    SM = (struct Socket *)shmat(shmid1, NULL, 0);
    fd_set readfds;
    // printf("\t\t\tReceiver process started\n");

    // Loop to wait for incoming messages
    while (1)
    {

        int maxfd = 0;

        // Initialize the file descriptor set
        FD_ZERO(&readfds);

        for (int i = 0; i < MAX_SOCKETS; i++)
        {
            if (SM[i].free == 1)
            {
                // printf("sock_id for FDSET: %d\n", SM[i].sock_id);
                FD_SET(SM[i].sock_id, &readfds);
                if (SM[i].sock_id > maxfd)
                {
                    // printf("maxfd: %d\n", SM[i].sock_id);
                    maxfd = SM[i].sock_id;
                }
            }
        }

        int ns = -1; // nospace flag, three values: 1: space not available, 0: space available, -1: no message received

        // Set the timeout for select() to NULL for now
        struct timeval timeout;
        timeout.tv_sec = 10; // check
        timeout.tv_usec = 0;

        // Call select() to check for incoming messages
        int activity = select(maxfd + 1, &readfds, NULL, NULL, &timeout);
        if (activity == -1)
        {
            perror("select failed");
            exit(1);
        }

        if (activity == 0)
        {
            // on timeout check whether a new MTP socket has been created and include it in the read/write set accordingly
            // for (int i = 0; i < MAX_SOCKETS; i++)
            // {
            //     if (SM[i].free == 1)
            //     {
            //         //printf("sock_id: %d\n", SM[i].sock_id);
            //         FD_SET(SM[i].sock_id, &readfds);
            //         if (SM[i].sock_id > maxfd)
            //         {
            //             maxfd = SM[i].sock_id;
            //         }
            //     }
            // }
        }
        else
        {
            for (int i = 0; i < MAX_SOCKETS; i++)
            {

                if (SM[i].free == 1 && FD_ISSET(SM[i].sock_id, &readfds))
                {
                    struct sockaddr_in dest_addr;
                    socklen_t len = sizeof(dest_addr);
                    char buffer[1024];
                    int n = recvfrom(SM[i].sock_id, buffer, 1024, 0, (struct sockaddr *)&dest_addr, &len);
                    // printf("recved message: %s\n", buffer);
                    // int dm = dropMessage(0.1);
                    // if (dm == 1)
                    // {
                    //     printf("Message dropped\n");
                    //     continue;
                    // }
                    // printf("fdisset = %d, i = %d\n, buffer = %s\n", FD_ISSET(SM[i].sock_id, &readfds), i, buffer);
                    if (n == -1)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            continue;
                        }
                        else
                        {
                            perror("recvfrom() failed");
                            exit(1);
                        }
                    }

                    if (buffer[0] == 'A' && buffer[1] == 'C' && buffer[2] == 'K')
                    {
                        char number[2];
                        memset(number, 0, sizeof(number));
                        number[0] = buffer[3];
                        number[1] = '\0';

                        int num = atoi(number);
                        int t = SM[i].sender_window.seq_number[0];

                        if (num != t)
                        {
                            continue;
                        }
                        SM[i].notyetack[num] = 0;
                        SM[i].sendbuf_size[num] = 0;
                        SM[i].sender_window.window_size += 1;

                        int first = SM[i].sender_window.seq_number[0];
                        for (int k = 0; k < 10; k++)
                        {
                            SM[i].sender_window.seq_number[k] = SM[i].sender_window.seq_number[(k + 1) % 10];
                        }
                        SM[i].sender_window.seq_number[9] = first;
                        memset(SM[i].sendbuf[num], 0, sizeof(SM[i].sendbuf[num]));

                        // printf("Received ACK for %d\n", num);
                        // printf("Sender window size: %d\n", SM[i].sender_window.window_size);
                    }
                    else if (buffer[0] == 'M' && buffer[1] == 'S' && buffer[2] == 'G')
                    {
                        struct sockaddr_in destaddr;
                        destaddr.sin_family = AF_INET;
                        destaddr.sin_port = htons(SM[i].destport);
                        inet_aton(SM[i].destip, &destaddr.sin_addr);

                        int num, j, x = 0;

                        char message[1024];
                        memset(message, 0, sizeof(message));
                        char number[2];
                        memset(number, 0, sizeof(number));

                        number[0] = buffer[3];
                        number[1] = '\0';
                        num = atoi(number);

                        // if (num == SM[i].receiver_window.seq_number[0])
                        // {
                        //     strcpy(message, buffer + 4);
                        //     printf("Received message: %s\n", message);
                        //     // strcpy(SM[i].recvbuf[j], message);
                        //     //check which entry is recv buffer is empty
                        //     for (j = 0; j < 5; j++)
                        //     {
                        //         if (SM[i].recvbuf_size[j] == 0)
                        //         {
                        //             strcpy(SM[i].recvbuf[j], message);
                        //             SM[i].recvbuf_size[j] = 1;
                        //             break;
                        //         }
                        //     }
                        //     memset(buffer, 0, sizeof(buffer));
                        //     SM[i].receiver_window.window_size--;
                        //     SM[i].receiver_window.last_inorder_seq_no = (SM[i].receiver_window.last_inorder_seq_no + 1) % 10;

                        //     printf("Window Size(Receiver) = %d\n", SM[i].receiver_window.window_size);
                        // }

                        for (j = 0; j < SM[i].receiver_window.window_size; j++)
                        {
                            int temp = SM[i].receiver_window.seq_number[j];
                            if (SM[i].recvbuf_size[temp % 5] == 0)
                            {
                                printf("S.no: %d\n", SM[i].receiver_window.seq_number[j]);
                                ns = 0;
                                int next = SM[i].receiver_window.seq_number[j];
                                // SM[i].recvbuf_size[next] = 0;

                                if (num == 1 + SM[i].receiver_window.last_inorder_seq_no)
                                {

                                    strcpy(message, buffer + 4);
                                    // printf("Received message: %s\n", message);
                                    strcpy(SM[i].recvbuf[j], message);
                                    memset(buffer, 0, sizeof(buffer));
                                    SM[i].receiver_window.window_size--;
                                    SM[i].receiver_window.last_inorder_seq_no = (SM[i].receiver_window.last_inorder_seq_no + 1) % 10;

                                    //printf("Window Size(Receiver) = %d\n", SM[i].receiver_window.window_size);

                                    // SM[i].receiver_window.window_size++;

                                    // printf("Sending to: %s:%d\n", SM[i].destip, SM[i].destport);

                                    // char acknowledge[1024];
                                    // memset(acknowledge, 0, sizeof(acknowledge));
                                    // sprintf(acknowledge, "ACK%d, rwnd size = %d", SM[i].receiver_window.last_inorder_seq_no, SM[i].receiver_window.window_size);
                                    // printf("%s\n", acknowledge);
                                    // SM[i].receiver_window.last_inorder_seq_no = (SM[i].receiver_window.last_inorder_seq_no + 1) % 5;
                                    // sendto(SM[i].sock_id, acknowledge, strlen(acknowledge), 0, (struct sockaddr *)&dest_addr, sizeof(dest_addr));
                                    // sleep(1);
                                }
                                // else if (buffer[0] == 'M' && buffer[1] == 'S' && buffer[2] == 'G')
                                // {
                                //     printf("Sending to: %s:%d\n", SM[i].destip, SM[i].destport);

                                //     char acknowledge[1024];
                                //     memset(acknowledge, 0, sizeof(acknowledge));
                                //     sprintf(acknowledge, "ACK%d, rwnd size = %d", SM[i].receiver_window.last_inorder_seq_no, SM[i].receiver_window.window_size);
                                //     printf("%s\n", acknowledge);
                                //     sendto(SM[i].sock_id, acknowledge, strlen(acknowledge), 0, (struct sockaddr *)&dest_addr, sizeof(dest_addr));
                                //     sleep(1);
                                // }
                                break;
                            }
                        }

                        if (ns == -1)
                        {
                            ns = 1;
                        }
                        else
                        {
                            // Space available in buffer and message received, now sending an ACK

                            // Send the pending message through the UDP sendto() call for the corresponding UDP socket
                            // printf("%d\n", SM[i].sock_id);
                            // printf("Sending to: %s:%d\n", SM[i].destip, SM[i].destport);

                            char acknowledge[1024];
                            memset(acknowledge, 0, sizeof(acknowledge));
                            sprintf(acknowledge, "ACK%d, rwnd size = %d", num, SM[i].receiver_window.window_size);
                            // printf("%s\n", acknowledge);
                            sendto(SM[i].sock_id, acknowledge, strlen(acknowledge), 0, (struct sockaddr *)&dest_addr, sizeof(dest_addr));
                            sleep(1);
                        }
                    }
                }
            }
        }
    }

    // Detach the shared memory segment
    if (shmdt(Sinfo) == -1)
    {
        perror("shmdt failed");
        exit(1);
    }

    if (shmdt(SM) == -1)
    {
        perror("shmdt failed");
        exit(1);
    }

    return NULL;
}

void *S(void *arg)
{
    time_t start_time, current_time, reference_time;
    // time(&start_time); // Record the start time

    // Get the shared memory segment containing socket information
    key_t key = ftok(".", 'b');
    int shmid = shmget(key, sizeof(struct SOCK_INFO), 0777 | IPC_CREAT);
    if (shmid == -1)
    {
        perror("shmget failed");
        exit(1);
    }

    struct SOCK_INFO *Sinfo = (struct SOCK_INFO *)shmat(shmid, NULL, 0);
    if (Sinfo == (struct SOCK_INFO *)-1)
    {
        perror("shmat failed");
        exit(1);
    }

    struct Socket *SM;
    int i;
    key_t key1;
    key1 = ftok(".", 'a');

    int shmid1 = shmget(key1, MAX_SOCKETS * sizeof(struct Socket), IPC_CREAT | 0777);
    SM = (struct Socket *)shmat(shmid1, NULL, 0);

    // printf("\t\t\tSender process started\n");
    time(&current_time);
    reference_time = current_time;
    int count = 0;
    // Loop to sleep for some time and wake up periodically
    while (1)
    {
        // Sleep for some time (less than T/2)
        // usleep(2500000); // Sleep for 2.5 seconds (2500 milliseconds)
        sleep(1);

        // if(count!=0){
        // Get the current time for checking message timeout period
        time(&current_time);
        //}

        for (i = 0; i < MAX_SOCKETS; i++)
        {
            // Check if the socket is free and there are unsent messages in the buffer
            if (SM[i].free == 1)
            {
                int j;
                for (j = 0; j < 10; j++)
                {
                    // Check if message needs to be resent due to timeout
                    if (strcmp(SM[i].sendbuf[j], "") != 0 && SM[i].notyetack[j] == 1 && SM[i].sender_window.window_size > 0)
                    {
                        // Calculate the elapsed time since message was last sent
                        // printf("Current time: %d\n", current_time - reference_time);
                        if (isTimeout(SM[i].send_time[j], current_time - reference_time))
                        {
                            // Retransmit the message
                            // Your retransmission code here...
                            // If yes, it retransmits all the messages within the current swnd for that MTP socket. It then checks the current swnd for each of the MTP sockets and determines whether there is a pending message from the sender-side message buffer that can be sent.
                            struct sockaddr_in destaddr;
                            destaddr.sin_family = AF_INET;
                            destaddr.sin_port = htons(SM[i].destport);
                            inet_aton(SM[i].destip, &destaddr.sin_addr);

                            char header[1024];
                            char *temp;
                            temp = (char *)malloc(1024 * sizeof(char));
                            memset(header, 0, sizeof(header));
                            sprintf(temp, "MSG%d%s", SM[i].sender_window.seq_number[j], SM[i].sendbuf[j]);

                            // printf("%s", temp);

                            ssize_t send_len = sendto(SM[i].sock_id, temp, strlen(temp), 0, (const struct sockaddr *)&destaddr, sizeof(destaddr));
                            if (send_len == -1)
                            {
                                perror("sendto failed 2");
                                exit(1);
                            }
                            // Update the send timestamp
                            time(&SM[i].send_time[j]);
                            SM[i].send_time[j] = SM[i].send_time[j] - reference_time;

                            // Remove the sent message from the sender-side message buffer
                            // free(SM[i].sendbuf[j]);
                            SM[i].sendbuf_size[j] = 1;
                            SM[i].notyetack[j] = 1;

                            // SM[i].sender_window.window_size--;
                            // printf("Window Size(Sender) = %d\n", SM[i].sender_window.window_size);
                        }
                    }
                }
            }
        }

        // Check the current swnd for each MTP socket

        for (i = 0; i < MAX_SOCKETS; i++)
        {
            // wait_sem(sem_mtx);
            if (SM[i].free == 1)
            {

                int j;
                for (j = 0; j < 10; j++)
                {
                    if (strcmp(SM[i].sendbuf[j], "") != 0 && SM[i].notyetack[j] == 0 && SM[i].sender_window.window_size > 0)
                    {
                        struct sockaddr_in destaddr;
                        destaddr.sin_family = AF_INET;
                        destaddr.sin_port = htons(SM[i].destport);
                        inet_aton(SM[i].destip, &destaddr.sin_addr);

                        char header[1024];
                        char *temp;
                        temp = (char *)malloc(1024 * sizeof(char));
                        memset(header, 0, sizeof(header));
                        sprintf(temp, "MSG%d%s", SM[i].sender_window.seq_number[j], SM[i].sendbuf[j]);

                        // printf("%s", temp);

                        ssize_t send_len = sendto(SM[i].sock_id, temp, strlen(temp), 0, (const struct sockaddr *)&destaddr, sizeof(destaddr));
                        if (send_len == -1)
                        {
                            perror("sendto failed 2");
                            exit(1);
                        }
                        // Update the send timestamp
                        time(&SM[i].send_time[j]);
                        SM[i].send_time[j] = SM[i].send_time[j] - reference_time;
                        // printf("Send time :%ld\n", SM[i].send_time[j]);

                        // Remove the sent message from the sender-side message buffer
                        // free(SM[i].sendbuf[j]);
                        SM[i].sendbuf_size[j] = 1;
                        SM[i].notyetack[j] = 1;

                        SM[i].sender_window.window_size--;
                        // printf("Window Size(Sender) = %d\n", SM[i].sender_window.window_size);
                        // break; // Exit loop after sending one message
                    }
                }
            }
            // signal_sem(sem_mtx);
        }
        // printf("Sender process woke up\n");
    }

    // Detach the shared memory segment
    if (shmdt(Sinfo) == -1)
    {
        perror("shmdt failed");
        exit(1);
    }

    if (shmdt(SM) == -1)
    {
        perror("shmdt failed");
        exit(1);
    }

    return NULL;
}

void *G(void *arg)
{
    key_t key;
    key = ftok(".", 'b');
    // create a shared memory segment
    int shmid = shmget(key, sizeof(struct SOCK_INFO), IPC_CREAT | 0777);
    if (shmid == -1)
    {
        perror("shmget failed");
        exit(1);
    }

    // attach the shared memory segment
    struct SOCK_INFO *Sinfo = (struct SOCK_INFO *)shmat(shmid, NULL, 0);
    if (Sinfo == (struct SOCK_INFO *)-1)
    {
        perror("shmat failed");
        exit(1);
    }

    // initialize the shared memory
    Sinfo->sock_id = 0;
    Sinfo->err_no = 0;
    Sinfo->port = 0;
    memset(Sinfo->ip, 0, sizeof(Sinfo->ip));

    // use the shared memory
    struct Socket *SM;
    int i;
    key_t key1;
    key1 = ftok(".", 'a');

    int shmid1 = shmget(key1, MAX_SOCKETS * sizeof(struct Socket), IPC_CREAT | 0777);
    SM = (struct Socket *)shmat(shmid1, NULL, 0);

    // Loop to clean up the corresponding entry in the MTP socket if the corresponding process is killed and the socket has not been closed explicitly
    while (1)
    {
        // wait_sem(sem_mtx);
        // Check if the corresponding process is killed and the socket has not been closed explicitly
        int i;
        for (i = 0; i < MAX_SOCKETS; i++)
        {
            if (SM[i].free == 1)
            {
                int status;
                int result = waitpid(SM[i].pid, &status, NULL);
                if (result == SM[i].pid)
                {
                    // The corresponding process is killed
                    // Clean up the corresponding entry in the MTP socket
                    close(SM[i].sock_id);
                    SM[i].free = 0;
                    SM[i].pid = 0;
                    SM[i].sock_id = 0;
                    SM[i].destport = 0;
                    memset(SM[i].destip, 0, sizeof(SM[i].destip));
                    memset(SM[i].sendbuf, 0, sizeof(SM[i].sendbuf));
                    memset(SM[i].recvbuf, 0, sizeof(SM[i].recvbuf));
                    memset(SM[i].sendbuf_size, 1, sizeof(SM[i].sendbuf_size)); // if empty, 1 else 0
                    memset(SM[i].recvbuf_size, 1, sizeof(SM[i].recvbuf_size));
                    memset(SM[i].sender_window.seq_number, 0, sizeof(SM[i].sender_window.seq_number));
                    memset(SM[i].receiver_window.seq_number, 0, sizeof(SM[i].receiver_window.seq_number));
                }
            }
        }
        // signal_sem(sem_mtx);
    }

    return NULL;
}

int main()
{

    key_t key;
    key = ftok(".", 'b');
    // create a shared memory segment
    int shmid = shmget(key, sizeof(struct SOCK_INFO), 0777 | IPC_CREAT);
    if (shmid == -1)
    {
        perror("shmget failed");
        exit(1);
    }

    // attach the shared memory segment
    struct SOCK_INFO *Sinfo = (struct SOCK_INFO *)shmat(shmid, NULL, 0);

    if (Sinfo == (struct SOCK_INFO *)-1)
    {
        perror("shmat failed");
        exit(1);
    }

    // initialize the shared memory
    Sinfo->sock_id = 0;
    Sinfo->err_no = 0;
    Sinfo->port = 0;
    memset(Sinfo->ip, 0, sizeof(Sinfo->ip));

    // use the shared memory
    struct Socket *SM;
    int i;
    key_t key1;
    key1 = ftok(".", 'a');

    int shmid1 = shmget(key1, MAX_SOCKETS * sizeof(struct Socket), IPC_CREAT | 0777);
    SM = (struct Socket *)shmat(shmid1, NULL, 0);

    for (i = 0; i < MAX_SOCKETS; i++)
    {
        if (SM == (struct Socket *)-1)
        {
            perror("shmat failed");
            exit(1);
        }
        // initialize the shared memory for each socket
        SM[i].free = 0;
        SM[i].pid = 0;
        SM[i].sock_id = 0;
        SM[i].destport = 0;

        memset(SM[i].destip, 0, sizeof(SM[i].destip));
        memset(SM[i].sendbuf, 0, sizeof(SM[i].sendbuf));
        memset(SM[i].recvbuf, 0, sizeof(SM[i].recvbuf));

        for (int j = 0; j < 10; j++)
        {
            SM[i].sendbuf_size[j] = 0;
            SM[i].notyetack[j] = 0;
        }
        for (int j = 0; j < 5; j++)
        {
            SM[i].recvbuf_size[j] = 0;
        }
        for (int j = 0; j < 10; j++)
        {
            SM[i].sender_window.seq_number[j] = j; // max window size = 10 and buffer size also 10? change it in future
        }
        for (int j = 0; j < 5; j++)
        {
            SM[i].receiver_window.seq_number[j] = j;
        }

        SM[i].sender_window.window_size = 5;
        SM[i].receiver_window.window_size = 5;
        SM[i].receiver_window.last_inorder_seq_no = -1;
        memset(SM[i].send_time, 0, sizeof(SM[i].send_time));
    }

    key_t semkeyA = ftok(".", 'A');
    int Sem1;
    Sem1 = semget(semkeyA, 1, IPC_CREAT | 0777);
    semctl(Sem1, 0, SETVAL, 0);

    key_t semkeyB = ftok(".", 'B');
    int Sem2;
    Sem2 = semget(semkeyB, 1, IPC_CREAT | 0777);
    semctl(Sem2, 0, SETVAL, 0);

    semctl(sem_mtx, 0, SETVAL, 0);
    // use the shared memory for the sockets
    pthread_t sender, receiver, g_collector;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&receiver, &attr, R, NULL);
    pthread_create(&sender, &attr, S, NULL);
    pthread_create(&g_collector, &attr, G, NULL);

    while (1)
    {
        wait_sem(Sem1);

        // check SOCK_INFO whether all fields are 0
        if (Sinfo->sock_id == 0 && Sinfo->err_no == 0 && Sinfo->port == 0)
        {
            int sock_id = socket(AF_INET, SOCK_DGRAM, 0);
            Sinfo->sock_id = sock_id;
            if (sock_id < 0)
            {
                Sinfo->err_no = errno;
            }

            signal_sem(Sem2);
        }
        else
        {
            int err;
            struct sockaddr_in servaddr;
            int sockfd = Sinfo->sock_id;
            servaddr.sin_family = AF_INET;
            servaddr.sin_port = htons(Sinfo->port);
            servaddr.sin_addr.s_addr = inet_addr(Sinfo->ip);

            if (bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0)
            {
                Sinfo->sock_id = -1;
                Sinfo->err_no = errno;
            }
            signal_sem(Sem2);
        }
    }

    // detach the shared memory segments
    for (i = 0; i < MAX_SOCKETS; i++)
    {
        if (shmdt(SM) == -1)
        {
            perror("shmdt failed");
            exit(1);
        }
    }

    // remove the shared memory segment
    if (shmctl(shmid, IPC_RMID, NULL) == -1)
    {
        perror("shmctl failed");
        exit(1);
    }

    // detach the shared memory segment
    if (shmdt(Sinfo) == -1)
    {
        perror("shmdt failed");
        exit(1);
    }

    // remove the shared memory segment
    if (shmctl(shmid, IPC_RMID, NULL) == -1)
    {
        perror("shmctl failed");
        exit(1);
    }

    return 0;
}
