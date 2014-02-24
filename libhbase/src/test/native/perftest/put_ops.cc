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

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include <hbase/async/hbase.h>

#include "common_utils.h"
#include "test_types.h"
#include "put_ops.h"

namespace hbase {
namespace test {

volatile uint64_t PutRunner::rpcsSinceLastFlush = 0;

void
PutRunner::WaitForCompletion() {
  while(semaphore_->NumAcquired() > 0) {
    usleep(20000);
  }
}

uint64_t
PutRunner::getRpcsSinceLastFlush() {
  return rpcsSinceLastFlush;
}

void
PutRunner::rpcsRpcsSinceLastFlushResetBy(uint64_t amount)  {
  rpcsSinceLastFlush -= amount;
}

void
PutRunner::Callback(int32_t err,
                    hb_client_t client,
                    hb_mutation_t mutation,
                    hb_result_t result,
                    void* extra) {
  RowSpec *rowSpec = reinterpret_cast<RowSpec*>(extra);
  PutRunner *runner = dynamic_cast<PutRunner *>(rowSpec->runner);

  runner->EndRpc(err);
  if (err) {
    if (err == ENOBUFS) {
      runner->Pause();
    }
    HBASE_LOG_MSG((err == ENOBUFS ? HBASE_LOG_LEVEL_TRACE : HBASE_LOG_LEVEL_ERROR),
        "Put failed for row \'%.*s\', result = %d.",
        rowSpec->key->length, rowSpec->key->buffer, err);
  } else {
    HBASE_LOG_TRACE("Put completed for row \'%.*s\'.",
                    rowSpec->key->length, rowSpec->key->buffer);
  }
  rowSpec->Destroy();
  hb_mutation_destroy(mutation);
  if (result) {
    hb_result_destroy(result);
  }
}

void PutRunner::BeginRpc() {
  if (paused_) {
    pthread_mutex_lock(&pauseMutex_);
    while(paused_) {
      pthread_cond_wait(&pauseCond_, &pauseMutex_);
    }
    pthread_mutex_unlock(&pauseMutex_);
  }
  semaphore_->Acquire();
}

void PutRunner::EndRpc(int32_t err)  {
  semaphore_->Release();
  if (paused_
      && (err == 0 || semaphore_->NumAcquired() == 0)) {
    pthread_mutex_lock(&pauseMutex_);
    if (paused_) {
      HBASE_LOG_INFO("Resuming PutRunner(0x%08x) operations.", tid_);
      paused_ = false;
    }
    pthread_cond_broadcast(&pauseCond_);
    pthread_mutex_unlock(&pauseMutex_);
  }
}

void PutRunner::Pause()  {
  if (!paused_) {
    pthread_mutex_lock(&pauseMutex_);
    if (!paused_) {
      HBASE_LOG_INFO("Pausing PutRunner(0x%08x) operations.", tid_);
      paused_ = true;
    }
    pthread_mutex_unlock(&pauseMutex_);
  }
}

void*
PutRunner::Run() {
  hb_put_t put = NULL;
  uint64_t endRow = startRow_ + numOps_;
  HBASE_LOG_INFO("Starting PutRunner(0x%08x).", tid_);

  for (uint64_t opNum = startRow_; opNum < endRow; ++opNum) {
    BeginRpc(); // ensures that we have permit to send a put

    RowSpec *rowSpec = new RowSpec();
    rowSpec->runner = this;
    rowSpec->key = generateRowKey(keyPrefix_, hashKeys_, opNum);
    hb_put_create(rowSpec->key->buffer, rowSpec->key->length, &put);
    hb_mutation_set_table(put, (const char *)table_->buffer, table_->length);
    hb_mutation_set_bufferable(put, bufferPuts_);
    hb_mutation_set_durability(put, (writeToWAL_ ? DURABILITY_USE_DEFAULT : DURABILITY_SKIP_WAL));

    cell_data_t *cell_data = new_cell_data();
    rowSpec->first_cell = cell_data;
    cell_data->value = bytebuffer_random(valueLen_);

    hb_cell_t *cell = (hb_cell_t*) calloc(1, sizeof(hb_cell_t));
    cell_data->hb_cell = cell;

    cell->row = rowSpec->key->buffer;
    cell->row_len = rowSpec->key->length;
    cell->family = family_->buffer;
    cell->family_len = family_->length;
    cell->qualifier = column_->buffer;
    cell->qualifier_len = column_->length;
    cell->value = cell_data->value->buffer;
    cell->value_len = cell_data->value->length;
    cell->ts = HBASE_LATEST_TIMESTAMP;

    hb_put_add_cell(put, cell);

    HBASE_LOG_TRACE("Sending row with row key : '%.*s'.", cell->row_len, cell->row);
    uint64_t startTime = currentTimeMicroSeconds();
    hb_mutation_send(client_, put, PutRunner::Callback, rowSpec);
    rpcsSinceLastFlush++; // need not be precise, used for flush throttling
    uint64_t endTime = currentTimeMicroSeconds();
    fnUpdateStats_(1, endTime - startTime, OP_PUT);
  }

  flush_client_and_wait(client_);
  HBASE_LOG_INFO("PutRunner(0x%08x) waiting for puts to complete.", tid_);
  WaitForCompletion();
  HBASE_LOG_INFO("PutRunner(0x%08x) complete.", tid_);
  return NULL;
}

} /* namespace test */
} /* namespace hbase */
