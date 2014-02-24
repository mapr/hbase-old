/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef HBASE_TESTS_PUT_OPS_H_
#define HBASE_TESTS_PUT_OPS_H_

#include <pthread.h>
#include <hbase/async/hbase.h>

#include "test_types.h"
#include "byte_buffer.h"

namespace hbase {
namespace test {

typedef void (*UpdateStatsFunction)(int, uint32_t, OpType);

class PutRunner : public OpRunner {
public:
  PutRunner(
      const hb_client_t client,
      const bytebuffer table,
      const uint64_t startRow,
      const uint64_t numOps,
      const bytebuffer family,
      const bytebuffer column,
      const char *keyPrefix,
      const int valueSize,
      const bool hashKeys,
      const bool bufferPuts,
      const bool writeToWAL,
      int32_t maxPendingRPCsPerThread,
      UpdateStatsFunction fnUpdateStats) :
        client_(client),
        table_ (table),
        startRow_(startRow),
        numOps_(numOps),
        hashKeys_(hashKeys),
        bufferPuts_(bufferPuts),
        writeToWAL_(writeToWAL),
        family_(family),
        column_(column),
        keyPrefix_(keyPrefix),
        valueLen_(valueSize),
        paused_(false),
        fnUpdateStats_(fnUpdateStats) {
    semaphore_ = new Semaphore(maxPendingRPCsPerThread);
    pthread_mutex_init(&pauseMutex_, 0);
    pthread_cond_init(&pauseCond_, 0);
  }

  ~PutRunner() {
    delete semaphore_;
  }

  void BeginRpc();

  void EndRpc(int32_t err);

  void Pause();

  static uint64_t getRpcsSinceLastFlush();

  static void rpcsRpcsSinceLastFlushResetBy(uint64_t amount);

protected:
  const hb_client_t client_;
  const bytebuffer table_;
  const uint64_t startRow_;
  const uint64_t numOps_;
  const bool hashKeys_;
  const bool bufferPuts_;
  const bool writeToWAL_;
  const bytebuffer family_;
  const bytebuffer column_;
  const char *keyPrefix_;
  const int valueLen_;

  volatile bool paused_;
  pthread_mutex_t pauseMutex_;
  pthread_cond_t pauseCond_;

  Semaphore *semaphore_;

  UpdateStatsFunction fnUpdateStats_;

  void *Run();

  void WaitForCompletion();

  static volatile uint64_t rpcsSinceLastFlush;

  static void Callback(int32_t err, hb_client_t client,
      hb_mutation_t mutation, hb_result_t result, void *extra);
};

} /* namespace test */
} /* namespace hbase */

#endif /* HBASE_TESTS_PUT_OPS_H_ */
