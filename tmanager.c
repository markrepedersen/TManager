#define _POSIX_C_SOURCE 1

#include "tmanager.h"
#include "msg.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

void usage(char *cmd) { printf("usage: %s  portNum\n", cmd); }

void initServer() {
  if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    perror("socket creation failed");
    exit(-1);
  }

  struct sockaddr_in servAddr;

  memset(&servAddr, 0, sizeof(servAddr));

  servAddr.sin_family = AF_INET;
  servAddr.sin_port = htons(port);
  servAddr.sin_addr.s_addr = INADDR_ANY;

  if (bind(sockfd, (const struct sockaddr *)&servAddr, sizeof(servAddr)) < 0) {
    perror("bind failed");
    exit(-1);
  }

  printf("Starting up Transaction Manager on %lu\n", port);
  printf("Port number:              %lu\n", port);
  printf("Log file name:            %s\n", logFileName);
}

void initLogFile() {
  snprintf(logFileName, sizeof(logFileName), "TXMG_%lu.log", port);
  logfileFD = open(logFileName, O_RDWR | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR);

  if (logfileFD < 0) {
    char msg[256];
    snprintf(msg, sizeof(msg), "Opening %s failed", logFileName);
    perror(msg);
    exit(-1);
  }

  struct stat fstatus;
  if (fstat(logfileFD, &fstatus) < 0) {
    perror("Filestat failed");
    exit(-1);
  }

  if (fstatus.st_size < sizeof(struct transactionSet)) {
    printf("Initializing the log file size\n");
    struct transactionSet tx;
    bzero(&tx, sizeof(tx));
    if (write(logfileFD, &tx, sizeof(tx)) != sizeof(tx)) {
      printf("Writing problem to log\n");
      exit(-1);
    }
  }
}

void initTransactionLog() {
  txlog = mmap(NULL, 512, PROT_READ | PROT_WRITE, MAP_SHARED, logfileFD, 0);

  if (txlog == NULL) {
    perror("Log file could not be mapped in:");
    exit(-1);
  }

  if (!txlog->initialized) {
    int i;
    for (i = 0; i < MAX_WORKERS; i++) {
      txlog->transaction[i].tstate = TX_NOTINUSE;
    }

    txlog->initialized = -1;
    // Make sure in memory copy is flushed to disk
    msync(txlog, sizeof(struct transactionSet), MS_SYNC | MS_INVALIDATE);
  }
}

void processArgs(int argc, char **argv) {
  if (argc != 2) {
    usage(argv[0]);
    exit(-1);
  }

  char *end;
  int err = 0;

  port = strtoul(argv[1], &end, 10);
  if (argv[1] == end) {
    printf("Port conversion error\n");
    exit(-1);
  }
}

void init(int argc, char **argv) {
  processArgs(argc, argv);
  initServer();
  initLogFile();
  initTransactionLog();
}

int receiveMessage(managerType *message, struct sockaddr_in *client) {
  socklen_t len;
  int n = recvfrom(sockfd, message, sizeof(managerType), MSG_WAITALL,
                   (struct sockaddr *)&client, &len);

  if (n < 0) {
    perror("Receiving error:");
    abort();
  }

  return n;
}

int sendMessage(struct sockaddr_in *client) {
  managerType message;
  int n = sendto(sockfd, &message, sizeof(managerType), 0,
                 (struct sockaddr *)client, client->sin_len);
  if (n < 0) {
    perror("Sending error");
    abort();
  }

  return n;
}

int isDuplicateTransaction(unsigned long tid) {
  int isDuplicate = 0;
  for (int i = 0; i < sizeof(txlog.transaction) / sizeof(txlog.transaction[0]);
       i++) {
    if (tid == txlog.transaction[i].txID) {
      isDuplicate = 1;
    }
  }
  return isDuplicate;
}

void processBegin(managerType *message, struct sockaddr_in *client) {
  if (isDuplicateTransaction(message->tid)) {
    sendMessage(client);
  } else {
    sendMessage(client);
  }
}

void processJoin(managerType *message, struct sockaddr_in *client) {
  if (!isDuplicateTransaction(message->tid)) {
    sendMessage(client);
  } else {
    sendMessage(client);
  }
}

void processMessage(managerType *message, struct sockaddr_in *client) {
  receiveMessage(message, client);
  switch (message->type) {
  case TXMSG_BEGIN:
    processBegin(message, client);
    break;
  case TXMSG_JOIN:
    processJoin(message, client);
    break;
  }
}

int main(int argc, char **argv) {
  init(argc, argv);

  int n;
  unsigned char buff[1024];
  for (int i = 0;; i = (++i % MAX_WORKERS)) {
    managerType message;
    struct sockaddr_in client;
    bzero(&client, sizeof(client));

    processMessage(&message, &client);

    txlog.transaction[i].worker[0] = client;
    // Make sure in memory copy is flushed to disk
    if (msync(&txlog, sizeof(struct transactionSet), MS_SYNC | MS_INVALIDATE)) {
      perror("Msync problem");
    }
  }

  sleep(1000);
}
