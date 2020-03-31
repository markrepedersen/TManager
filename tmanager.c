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

void logToFile() {
  if (msync(&txlog, sizeof(struct transactionSet), MS_SYNC | MS_INVALIDATE)) {
    perror("Msync problem");
  }
}

void initTransactionLog() {
  txlog = mmap(NULL, 512, PROT_READ | PROT_WRITE, MAP_SHARED, logfileFD, 0);

  if (txlog == NULL) {
    perror("Log file could not be mapped in:");
    exit(-1);
  }

  if (!txlog->initialized) {
    for (int i = 0; i < MAX_WORKERS; i++) {
      txlog->transaction[i].tstate = TX_NOTINUSE;
    }

    txlog->initialized = -1;
    logToFile();
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

int receiveMessage(managerType *message, struct sockaddr_in *client) {
  socklen_t len;
  int n = recvfrom(sockfd, message, sizeof(managerType), MSG_DONTWAIT,
                   (struct sockaddr *)&client, &len);

  if (n < 0) {
    perror("Receiving error:");
    abort();
  }

  return n;
}

int sendMessage(managerType *message, struct sockaddr_in *client) {
  int n = sendto(sockfd, message, sizeof(managerType), 0,
                 (struct sockaddr *)client, sizeof(struct sockaddr_in));
  if (n < 0) {
    perror("Sending error");
    abort();
  }

  return n;
}

int getTransactionById(unsigned long txId) {
  for (int i = 0; i < sizeof txlog->transaction / sizeof txlog->transaction[0];
       i++) {
    if (txlog->transaction[i].txID == txId) {
      return i;
    }
  }
}

void setTransactionState(unsigned long txId, enum txState state) {
  for (int i = 0;
       i < sizeof(txlog->transaction) / sizeof(txlog->transaction[0]); i++) {
    if (txlog->transaction[i].txID == txId) {
      txlog->transaction[i].tstate = state;
    }
  }
}

void setTransactionTimer(unsigned long txId, time_t timer) {
  for (int i = 0;
       i < sizeof(txlog->transaction) / sizeof(txlog->transaction[0]); i++) {
    if (txlog->transaction[i].txID == txId) {
      txlog->transaction[i].timer = timer;
    }
  }
}

int isTransactionInUse(unsigned long tid) {
  int isDuplicate = 0;
  for (int i = 0;
       i < sizeof(txlog->transaction) / sizeof(txlog->transaction[0]); i++) {
    if (tid == txlog->transaction[i].txID) {
      isDuplicate = 1;
    }
  }
  return isDuplicate;
}

int getNumWorkers(int i) {
  return sizeof txlog->transaction[i].workers /
         sizeof txlog->transaction[i].workers[0];
}

int getNumAnswers(int i) {
  return txlog->transaction[i].answers;
}

worker *getWorkers(unsigned long tid) {
  for (int i = 0;
       i < sizeof(txlog->transaction) / sizeof(txlog->transaction[0]); i++) {
    if (tid == txlog->transaction[i].txID) {
      return txlog->transaction[i].workers;
    }
  }
  return NULL;
}

void processCommit(managerType *message, struct sockaddr_in *client) {
  worker *workers = getWorkers(message->tid);
  setTransactionTimer(message->tid, time(NULL) + TIMEOUT);
  for (int i = 0; i < sizeof *workers / sizeof workers[0]; i++) {
    message->type = TXMSG_PREPARE_TO_COMMIT;
    sendMessage(message, &workers->client);
    setTransactionState(message->tid, TX_VOTING);
  }
}

int getVoteResult(unsigned long tid, int numWorkers) {
  int numVotes = 0;
  worker *workers = getWorkers(tid);
  for (int i = 0; i < numWorkers; i++) {
    numVotes += workers[i].vote;
  }

  return numVotes >= numWorkers;
}

void setWorkerVote(unsigned long tid) {
  for (int i = 0; i < MAX_TX; i++) {
    txlog->transaction[i].answers++;
    if (txlog->transaction[i].txID == tid) {
      for (int j = 0; j < MAX_WORKERS; j++) {
        txlog->transaction[i].workers[j].vote = 1;
      }
    }
  }
}

void processCommitVote(managerType *message, struct sockaddr_in *client) {
  setWorkerVote(message->tid);

  int index = getTransactionById(message->tid);
  int numWorkers = getNumWorkers(index);
  int numAnswers = getNumAnswers(index);
    
  if (numAnswers >= numWorkers) {
    int voteResult = getVoteResult(message->tid, numWorkers);
    if (voteResult == 1) {
      // All nodes voted yes.
      message->type = TXMSG_COMMITTED;
      sendMessage(message, client);
    } else {
      //TODO:  >= 1 node voted no.
    }
  }
}

void processAbortVote(managerType *message, struct sockaddr_in *client) {
  setWorkerVote(message->tid);
  
  int index = getTransactionById(message->tid);
  int numWorkers = getNumWorkers(index);
  int numReplies = getNumAnswers(index);  
  
  if (numReplies >= numWorkers) {
    int voteResult = getVoteResult(message->tid, numWorkers);
    if (voteResult == 1) {
      // All nodes voted yes.
      message->type = TXMSG_ABORTED;
      sendMessage(message, client);
    } else {
      //TODO:  >= 1 node voted no.
    }
  }
}

void processCommitCrash(managerType *message, struct sockaddr_in *client) {
  abort();
}

void processAbort(managerType *message, struct sockaddr_in *client) {
  worker *workers = getWorkers(message->tid);
  setTransactionTimer(message->tid, time(NULL) + TIMEOUT);
  for (int i = 0; i < sizeof(*workers) / sizeof(workers[0]); i++) {
    message->type = TXMSG_ABORTED;
    sendMessage(message, &workers[i].client);
    setTransactionState(message->tid, TX_VOTING);
  }
}

void processAbortCrash(managerType *message, struct sockaddr_in *client) {
  abort();
}

void processBegin(managerType *message, struct sockaddr_in *client) {
  if (isTransactionInUse(message->tid)) {
    message->type = TXMSG_TID_IN_USE;
    sendMessage(message, client);
  } else {
    message->type = TXMSG_TID_OK;
    sendMessage(message, client);
  }
  setTransactionState(message->tid, TX_INPROGRESS);
}

void processJoin(managerType *message, struct sockaddr_in *client) {
  if (!isTransactionInUse(message->tid)) {
    message->type = TXMSG_TID_OK;
    sendMessage(message, client);
  } else {
    message->type = TXMSG_TID_IN_USE;
    sendMessage(message, client);
  }
}

void processMessage(managerType *message, struct sockaddr_in *client) {
  receiveMessage(message, client);
  txlog->initialized = 1;

  switch (message->type) {
  case TXMSG_BEGIN:
    processBegin(message, client);
    break;
  case TXMSG_JOIN:
    processJoin(message, client);
    break;
  case TXMSG_COMMIT_REQUEST:
    processCommit(message, client);
    break;
  case TXMSG_COMMIT_CRASH_REQUEST:
    processCommitCrash(message, client);
    break;
  case TXMSG_ABORT_REQUEST:
    processAbort(message, client);
    break;
  case TXMSG_ABORT_CRASH_REQUEST:
    processAbortCrash(message, client);
    break;
  case TXMSG_VOTE_COMMIT:
    processCommitVote(message, client);
    break;
  case TXMSG_VOTE_ABORT:
    processAbortVote(message, client);
    break;
  default:
    // No message received -> do nothing.
    break;
  }
}

int isTransactionTimedOut(int i) {
  if (txlog->transaction[i].timer != -1 &&
      time(NULL) > txlog->transaction[i].timer) {
    return 1;
  }
  return 0;
}

void resetTimer(int i) {
  txlog->transaction[i].timer = -1;
  txlog->transaction[i].answers = 0;
}

int main(int argc, char **argv) {
  processArgs(argc, argv);
  initServer();
  initLogFile();
  initTransactionLog();

  for (int i = 0;; i = (++i % MAX_TX)) {
    if (isTransactionTimedOut(i)) {
      // Voting was short circuited due to >= 1 node failing to respond to vote.
      resetTimer(i);
    } else {
      managerType message;
      struct sockaddr_in client;
      bzero(&client, sizeof(client));

      processMessage(&message, &client);
    }
  }
}
