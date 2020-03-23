
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "msg.h"
#include "tworker.h"

static struct logFile *log;
static int delay = 0;
static int cmdSock;
static int txSock;

void usage(char *cmd) {
    printf("usage: %s  cmdportNum txportNum\n",
           cmd);
}

void setup(int argc, char **argv) {
    if (argc != 3) {
        usage(argv[0]);
        exit(-1);
    }
    unsigned long cmdPort;
    unsigned long txPort;

    char *end;
    cmdPort = strtoul(argv[1], &end, 10);
    if (argv[1] == end) {
        printf("Command port conversion error\n");
        exit(-1);
    }
    txPort = strtoul(argv[2], &end, 10);
    if (argv[2] == end) {
        printf("Transaction port conversion error\n");
        exit(-1);
    }

    char logFileName[128];
    /* got the port number create a logfile name */
    snprintf(logFileName, sizeof(logFileName), "TXworker_%lu.log", cmdPort);

    int logfileFD;

    logfileFD = open(logFileName, O_RDWR | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR);
    if (logfileFD < 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Opening %s failed", logFileName);
        perror(msg);
        exit(-1);
    }

    // check the logfile size
    struct stat fstatus;
    if (fstat(logfileFD, &fstatus) < 0) {
        perror("Filestat failed");
        exit(-1);
    }

    // Let's see if the logfile has some entries in it by checking the size
    if (fstatus.st_size < sizeof(struct logFile)) {
        // Just write out a zeroed file struct
        printf("Initializing the log file size\n");
        struct logFile tx;
        bzero(&tx, sizeof(tx));
        if (write(logfileFD, &tx, sizeof(tx)) != sizeof(tx)) {
            printf("Writing problem to log\n");
            exit(-1);
        }
    }

    // Now map the file in.
    log = mmap(NULL, 512, PROT_READ | PROT_WRITE, MAP_SHARED, logfileFD, 0);
    if (log == NULL) {
        perror("Log file could not be mapped in:");
        exit(-1);
    }

    // Some demo data
    // strncpy(log->txData.IDstring, "Hi there!! ¯\\_(ツ)_/¯", IDLEN);
    // log->txData.A = 10;
    // log->txData.B = 100;
    // log->log.oldA = 83;
    // log->log.newA = 10;
    // log->log.oldB = 100;
    // log->log.newB = 1023;
    // log->initialized = -1;
    // flush();

    // Some demo data
    // strncpy(log->log.newIDstring, "1234567890123456789012345678901234567890", IDLEN);

    cmdSock = socket(AF_INET, SOCK_DGRAM, 0);
    if (cmdSock < 0) {
        perror("Could not open socket");
        exit(-1);
    }

    txSock = socket(AF_INET, SOCK_DGRAM, 0);
    if (txSock < 0) {
        perror("Could not open socket");
        exit(-1);
    }

    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cmdPort);
    if (bind(cmdSock, (struct sockaddr*) &addr, sizeof(addr)) < 0) {
        perror("Could not bind socket");
        exit(-1);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(txPort);
    if (bind(txSock, (struct sockaddr*) &addr, sizeof(addr)) < 0) {
        perror("Could not bind socket");
        exit(-1);
    }

    printf("Command port:  %lu\n", cmdPort);
    printf("TX port:       %lu\n", txPort);
    printf("Log file name: %s\n", logFileName);
}

// Flush changes to the log file.
void flushLog() {
    if (msync(log, sizeof(struct logFile), MS_SYNC | MS_INVALIDATE)) {
        perror("Msync problem");
        exit(-1);
    }
}

/**
 * Check for incoming commands.
 * Returns a pointer to the command, or NULL if none was received.
 */
const msgType* receiveCommand() {
    static msgType command;
    const int cmdSize = sizeof(command);
    int res = recvfrom(cmdSock, &command, cmdSize, MSG_DONTWAIT, NULL, NULL);
    if (res == cmdSize) return &command;
    if (res != -1) printf("Received packet with invalid size: %d\n", res);
    else if (errno != EAGAIN && errno != EWOULDBLOCK) perror("Receive cmd error");
    return NULL;
}

const char* getCommandTypeString(int msgType) {
    static const char* names[] = {
        "BEGINTX",
        "JOINTX",
        "NEW_A",
        "NEW_B",
        "NEW_ID",
        "DELAY_RESPONSE",
        "CRASH",
        "COMMIT",
        "COMMIT_CRASH",
        "ABORT",
        "ABORT_CRASH",
        "VOTE_ABORT"
    };
    return names[msgType - BEGINTX];
}

void printCommand(const msgType* command) {
    printf("%s: ", getCommandTypeString(command->msgID));
    printf("tid: %u, ", command->tid);
    printf("port: %u, ", command->port);
    printf("newVal: %d, ", command->newValue);
    printf("delay: %d, ", command->delay);
    printf("str: %s\n", command->strData.newID);
}

void handleCommand(const msgType* command) {
    if (!command) return;
    const int msgType = command->msgID;
    if (msgType < BEGINTX || msgType > VOTE_ABORT) {
        printf("Received invalid command type: %d\n", msgType);
        return;
    }
    printCommand(command);
    switch (command->msgID) {
        case BEGINTX:
            break;
        case JOINTX:
            break;
        case NEW_A:
            break;
        case NEW_B:
            break;
        case NEW_IDSTR:
            break;
        case DELAY_RESPONSE:
            break;
        case CRASH:
            _exit(-1);
            break;
        case COMMIT:
            break;
        case COMMIT_CRASH:
            break;
        case ABORT:
            break;
        case ABORT_CRASH:
            break;
        case VOTE_ABORT:
            break;
    }
}

int main(int argc, char **argv) {
    setup(argc, argv);
    int flags = MSG_DONTWAIT;

    while (1) {
        handleCommand(receiveCommand());
    }
}
