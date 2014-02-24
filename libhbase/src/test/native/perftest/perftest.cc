/**
 * Copyright The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <hbase/sync/hbase.h>
#include <hbase/async/hbase.h>

#include "admin_ops.h"
#include "common_utils.h"
#include "test_types.h"
#include "byte_buffer.h"

#include "put_ops.h"

namespace hbase {
namespace test {

static bool argCreateTable = false;
static bool argHashKeys    = true;
static bool argBufferPuts  = true;
static bool argWriteToWAL  = true;

static char *argZkQuorum    = (char*) "localhost:2181";
static char *argZkRootNode  = NULL;
static char *argTableName   = (char*) "test_table";
static char *argFamilyName  = (char*) "f";
static char *argLogFilePath = NULL;
static char *argKeyPrefix   = (char*) "user";

static uint64_t argStartRow       = 1;
static uint64_t argNumOps         = 1000000;
static uint32_t argValueSize      = 1024;
static uint32_t argFlushBatchSize = 0;
static uint32_t argMaxPendingRPCs = 100000;
static uint32_t argNumThreads     = 1;

static
void usage() {
  fprintf(stderr, "Usage: perftest [options]...\n"
      "Available options: (default values in [])\n"
      "  -zkQuorum <zk_quorum> [localhost:2181]\n"
      "  -zkRootNode <zk_root_node> [/hbase]\n"
      "  -table <table> [test_table]\n"
      "  -family <family> [f]\n"
      "  -startRow <start_row> [1]\n"
      "  -valueSize <value_size> [1024]\n"
      "  -numOps <numops> [1000000]\n"
      "  -keyPrefix <key_prefix> [user]\n"
      "  -hashKeys true|false [true]\n"
      "  -bufferPuts true|false [true]\n"
      "  -writeToWAL true|false [true]\n"
      "  -flushBatchSize <flush_batch_size> [0(disabled)]\n"
      "  -maxPendingRPCs <max_pending_rpcs> [100000]\n"
      "  -numThreads <num_threads> [1]\n"
      "  -createTable true|false [false]\n"
      "  -logFilePath <log_file_path> [stderr]\n");
  exit(1);
}

static void
parseArgs(int argc,
          char *argv[]) {
  // skip program name
  argc--; argv++;
#define ArgEQ(a) (argc > 1 && (strcmp(argv[0], a) == 0))
  while (argc >= 1) {
    if (ArgEQ("-valueSize")) {
      argValueSize = atoi(argv[1]);
    } else if (ArgEQ("-numOps")) {
      argNumOps = atol(argv[1]);
    } else if (ArgEQ("-startRow")) {
      argStartRow = atol(argv[1]);
    } else if (ArgEQ("-table")) {
      argTableName = argv[1];
    } else if (ArgEQ("-family")) {
      argFamilyName = argv[1];
    } else if (ArgEQ("-keyPrefix")) {
      argKeyPrefix = argv[1];
    } else if (ArgEQ("-createTable")) {
      argCreateTable = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-hashKeys")) {
      argHashKeys = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-bufferPuts")) {
      argBufferPuts = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-writeToWAL")) {
      argWriteToWAL = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-zkQuorum")) {
      argZkQuorum = argv[1];
    } else if (ArgEQ("-zkRootNode")) {
      argZkRootNode = argv[1];
    } else if (ArgEQ("-logFilePath")) {
      argLogFilePath = argv[1];
    } else if (ArgEQ("-flushBatchSize")) {
      // if not set to 0, starts a thread to
      // flush after every 'flushBatchSize' RPCs
      argFlushBatchSize = atoi(argv[1]);
    } else if (ArgEQ("-maxPendingRPCs")) {
      argMaxPendingRPCs = atoi(argv[1]);
    } else if (ArgEQ("-numThreads")) {
      argNumThreads = atoi(argv[1]);
    } else {
      usage();
    }
    argv += 2; argc -= 2;
  }
#undef ArgEQ

  if (!argBufferPuts && argFlushBatchSize > 0) {
    fprintf(stderr, "'-flushBatchSize' can not be specified if '-bufferPuts' is false");
    exit(1);
  }
}

static volatile bool g_stopRun = false;

static volatile uint64_t g_numOps[OP_LAST] = {0};
static volatile uint64_t g_cumLatencyOps[OP_LAST] = {0};
static volatile uint64_t g_maxLatencyOps[OP_LAST] = {0};
static volatile uint64_t g_minLatencyOps[OP_LAST] = {0};

static pthread_mutex_t g_statsMutexes[OP_LAST] = {
    PTHREAD_MUTEX_INITIALIZER,
    PTHREAD_MUTEX_INITIALIZER,
    PTHREAD_MUTEX_INITIALIZER
};

static void
UpdateStats(int numOps, uint32_t elapsed, OpType opType) {
  pthread_mutex_lock(&g_statsMutexes[opType]);
  if (g_minLatencyOps[opType] == 0 && g_maxLatencyOps[opType] == 0) {
    g_minLatencyOps[opType] = g_maxLatencyOps[opType] = elapsed;
  }
  g_numOps[opType] += numOps;
  g_cumLatencyOps[opType] += elapsed;
  if (g_maxLatencyOps[opType] < elapsed) {
    g_maxLatencyOps[opType] = elapsed;
  }
  if (elapsed < g_minLatencyOps[opType]) {
    g_minLatencyOps[opType] = elapsed;
  }
  pthread_mutex_unlock(&g_statsMutexes[opType]);
}

static void *
StatusThread(void *arg) {
  uint64_t prevNumOps[OP_LAST] = {0};
  uint64_t currentNumOps[OP_LAST] = {0};
  uint64_t totalOps;
  uint64_t totalOpsLastSec;
  int nsec = 0;

  while (!g_stopRun) {
    sleep(1);
    ++nsec;
    time_t t;
    time(&t);
    struct tm *timeinfo = localtime(&t);
    int hour = timeinfo->tm_hour;
    int min = timeinfo->tm_min;
    int secs = timeinfo->tm_sec;

    totalOps = totalOpsLastSec = 0;
    for (int i = 0; i < OP_LAST; ++i) {
      currentNumOps[i] = g_numOps[i] - prevNumOps[i];
      totalOps += g_numOps[i];
      totalOpsLastSec += currentNumOps[i];
    }

    if ((nsec % 10) == 1) {
      fprintf(stdout, "%8s %5s %9s %6s",
          "Time", "Secs", "TotalOps", "Ops/s");
      for (int i = 0; i < OP_LAST; ++i) {
        uint64_t numOpsForOp = g_numOps[i];
        if (!numOpsForOp) continue;
        fprintf(stdout, "|%6s(#) %6s %8s %8s %8s",
                OP_TYPE_NAMES[i], "Ops/s",
                "avg(us)", "max(us)", "min(us)");
      }
      fprintf(stdout, "|\n");
      fflush(stdout);
    }

    fprintf(stdout, "%02d:%02d:%02d %5d %9"PRIu64" %6"PRIu64"",
            hour, min, secs, nsec, totalOps, totalOpsLastSec);
    for (int i = 0; i < OP_LAST; ++i) {
      uint64_t numOpsForOp = g_numOps[i];
      if (!numOpsForOp) continue;
      fprintf(stdout, "|%9"PRIu64" %6"PRIu64" %8"PRIu64" %8"PRIu64" %8"PRIu64"",
              numOpsForOp, currentNumOps[i],
              (g_cumLatencyOps[i] / (numOpsForOp ? numOpsForOp : 1)),
              g_maxLatencyOps[i], g_minLatencyOps[i]);
    }
    fprintf(stdout, "|\n");
    fflush(stdout);
    for (int i = 0; i < OP_LAST; ++i) {
      prevNumOps[i] = g_numOps[i];
    }
  }

  for (int i = 0; i < OP_LAST; ++i) {
    uint64_t numOpsForOp = g_numOps[i];
    if (!numOpsForOp) continue;
    fprintf(stdout, "%7s %10"PRIu64" Ops, %"PRIu64" ops/s. "
            "Latency(us): avg %"PRIu64", max %"PRIu64", min %"PRIu64".\n",
            OP_TYPE_NAMES[i], numOpsForOp, (numOpsForOp/nsec),
            (g_cumLatencyOps[i] / (numOpsForOp ? numOpsForOp : 1)),
            g_maxLatencyOps[i], g_minLatencyOps[i]);
  }
  fflush(stdout);

  return NULL;
}

struct FlushSpec {
  const hb_client_t client_;
  const uint64_t flushBatchSize_;
};

void*
FlushThread(void *arg) {
  FlushSpec *spec = reinterpret_cast<FlushSpec*>(arg);
  while(!g_stopRun) {
    usleep(20000); // sleep for 20 milliseconds
    uint64_t rpcs_in_flight = PutRunner::getRpcsSinceLastFlush();
    if (rpcs_in_flight > spec->flushBatchSize_) {
      uint64_t recordedTime = currentTimeMicroSeconds();
      flush_client_and_wait(spec->client_);
      PutRunner::rpcsRpcsSinceLastFlushResetBy(rpcs_in_flight);
      UpdateStats(1, currentTimeMicroSeconds() - recordedTime, OP_FLUSH);
    }
  }
  return NULL;
}

void *RunPuts(void *arg) {
  PutRunner *runner = reinterpret_cast<PutRunner*>(arg);
  return runner->Start();
}

/**
 * Program entry point
 */
extern "C" int
main(int argc,
    char *argv[]) {
  if (argc == 1) usage();

  int32_t retCode = 0;
  FILE *logFile = NULL;
  hb_connection_t connection = NULL;
  hb_client_t client = NULL;
  bytebuffer table = NULL, column = NULL;
  bytebuffer families[1];

  parseArgs(argc, argv);

  uint64_t opsPerThread = (argNumOps/argNumThreads);
  int32_t maxPendingRPCsPerThread = argMaxPendingRPCs/argNumThreads;
  if (maxPendingRPCsPerThread > SEM_VALUE_MAX) {
    fprintf(stderr, "Can not have more than %d pending RPCs per thread.",
            SEM_VALUE_MAX);
    exit(1);
  }

  table = bytebuffer_strcpy(argTableName);
  families[0] = bytebuffer_strcpy(argFamilyName);
  column = bytebuffer_strcpy("a");

  hb_log_set_level(HBASE_LOG_LEVEL_DEBUG); // defaults to INFO
  if (argLogFilePath != NULL) {
    logFile = fopen(argLogFilePath, "a");
    if (!logFile) {
      retCode = errno;
      fprintf(stderr, "Unable to open log file \"%s\"", argLogFilePath);
      perror(NULL);
      goto cleanup;
    }
    hb_log_set_stream(logFile); // defaults to stderr
  }

  if ((retCode = hb_connection_create(argZkQuorum, argZkRootNode, &connection))) {
    HBASE_LOG_ERROR("Could not create HBase connection : errorCode = %d.", retCode);
    goto cleanup;
  }

  if ((retCode = ensureTable(connection,
      argCreateTable, argTableName, families, 1)) != 0) {
    HBASE_LOG_ERROR("Failed to ensure table %s : errorCode = %d",
        argTableName, retCode);
    goto cleanup;
  }

  HBASE_LOG_INFO("Connecting to HBase cluster using Zookeeper ensemble '%s'.",
                 argZkQuorum);
  if ((retCode = hb_client_create(connection, &client)) != 0) {
    HBASE_LOG_ERROR("Could not connect to HBase cluster : errorCode = %d.", retCode);
    goto cleanup;
  }

  // launch threads
  {
    pthread_t statusThreadId, flushThreadId;
    pthread_t workers[argNumThreads];
    PutRunner *runner[argNumThreads];

    for (size_t i = 0; i < argNumThreads; ++i) {
      runner[i] = new PutRunner(client, table,
          (argStartRow + (i*opsPerThread)), opsPerThread,
          families[0], column, argKeyPrefix, argValueSize,
          argHashKeys, argBufferPuts, argWriteToWAL,
          maxPendingRPCsPerThread, UpdateStats);
      pthread_create(&workers[i], 0, RunPuts, (void*)runner[i]);
    }

    // status thread
    pthread_create(&statusThreadId, 0, StatusThread, NULL);

    if (argFlushBatchSize) {
      FlushSpec flushSpec = {client, argFlushBatchSize};
      pthread_create(&flushThreadId, 0, FlushThread, &flushSpec);
    }

    for (size_t i = 0; i < argNumThreads; ++i) {
      pthread_join(workers[i], NULL);
      delete runner[i];
    }

    g_stopRun = true;
    pthread_join(statusThreadId, NULL);
    if (argFlushBatchSize) {
      pthread_join(flushThreadId, NULL);
    }
  }

cleanup:
  if (client) {
    disconnect_client_and_wait(client);
  }

  if (connection) {
    hb_connection_destroy(connection);
  }

  if (column) {
    bytebuffer_free(column);
  }

  if (families[0]) {
    bytebuffer_free(families[0]);
  }

  if (table) {
    bytebuffer_free(table);
  }

  if (logFile) {
    fclose(logFile);
  }

  return retCode;
}

} /* namespace test */
} /* namespace hbase */
