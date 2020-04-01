#include <netinet/in.h>
#include <stdio.h>

#ifndef TMANAGER_h
#define TMANGER_h 100
#define MAX_WORKERS 6
#define MAX_TX 4
#define TIMEOUT 10

typedef enum txState {
  TX_NOTINUSE = 100,
  TX_INPROGRESS,
  TX_VOTING,
  TX_ABORTED,
  TX_COMMITTED
} transactionState;

typedef struct worker {
  struct sockaddr_in client;
  int initialized;
} worker;

typedef struct tx {
  unsigned long txID;
  transactionState tstate;
  time_t timer;
  worker workers[MAX_WORKERS];
  int numWorkers;
  int pendingCrash;
  int numAnswers;
  int numYesVotes;
} transaction;

typedef struct transactionSet {
  int initialized;
  transaction transaction[MAX_TX];
} transactionSet;

int sockfd;
unsigned long port;
char logFileName[128];
int logfileFD;
transactionSet *txlog;

#endif
