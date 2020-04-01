#ifndef MSG_H
#define MSG_H 1
#include <stdint.h>
#include "tworker.h"

// Note that this file defines the packet format and message types for messages from
// the cmd program to a worker. It does not define the message format between the worker
// and transaction manager that is a design decision for the implementator.

#define HOSTLEN 512

enum cmdMsgKind {
    BEGINTX = 1000,
    JOINTX,
    NEW_A,
    NEW_B,
    NEW_IDSTR,
    DELAY_RESPONSE,
    CRASH,
    COMMIT,
    COMMIT_CRASH,
    ABORT,
    ABORT_CRASH,
    VOTE_ABORT
};

enum txMsgKind {
    TXMSG_BEGIN = 1000,
    TXMSG_JOIN,
    TXMSG_TID_OK,
    TXMSG_TID_BAD,
    TXMSG_COMMIT_REQUEST,
    TXMSG_COMMIT_CRASH_REQUEST,
    TXMSG_ABORT_REQUEST,
    TXMSG_ABORT_CRASH_REQUEST,
    TXMSG_PREPARE_TO_COMMIT,
    TXMSG_VOTE_COMMIT,
    TXMSG_VOTE_ABORT,
    TXMSG_COMMITTED,
    TXMSG_POLL_RESULT,
    TXMSG_ABORTED
};

typedef struct {
    uint32_t tid;
    uint32_t type;
} managerType;

// The following is not the best approach/format for the command messages
// but it is simple and it will fit in one packet. Which fields have
// usable values will depend upon the type of the command message.

typedef struct {
    uint32_t msgID;
    uint32_t tid;  // Transaction ID
    uint32_t port;
    int32_t newValue;  // New value for A or B
    int32_t delay;
    union {  // string data
        char newID[IDLEN];
        char hostName[HOSTLEN];
    } strData;
} msgType;

#endif
