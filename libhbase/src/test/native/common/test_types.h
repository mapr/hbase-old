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
#ifndef HBASE_TESTS_TYPES_H_
#define HBASE_TESTS_TYPES_H_

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdlib.h>

#include "byte_buffer.h"

namespace hbase {
namespace test {

typedef struct cell_data_t_ {
  bytebuffer value;
  hb_cell_t  *hb_cell;
  struct cell_data_t_ *next_cell;
} cell_data_t;

inline cell_data_t*
new_cell_data() {
  cell_data_t *cell_data = (cell_data_t*) calloc(1, sizeof(cell_data_t));
  cell_data->next_cell = NULL;
  return cell_data;
}

typedef enum {
  OP_PUT = 0,
  OP_GET,
  OP_FLUSH,
  OP_LAST
} OpType;

extern const char* OP_TYPE_NAMES[];

class Semaphore {
public:
  Semaphore(uint32_t numPermits) :
      numPermits_(numPermits) {
    sem_init(&sem_, 0, numPermits);
  }

  ~Semaphore() {
    sem_destroy(&sem_);
  }

  void Acquire(uint32_t num = 1) {
    for (uint32_t i = 0; i < num; ++i) {
      sem_wait(&sem_);
    }
  }

  void Release(uint32_t num = 1) {
    for (uint32_t i = 0; i < num; ++i) {
      sem_post(&sem_);
    }
  }

  uint32_t NumAcquired() {
    int semVal = 0;
    sem_getvalue(&sem_, &semVal);
    return (numPermits_ - semVal);
  }

  uint32_t Drain() {
    uint32_t permits = 0;
    while (sem_trywait(&sem_) == 0) {
      ++permits;
    }
    return permits;
  }

private:
  uint32_t numPermits_;
  sem_t sem_;
};

typedef void *(*PThreadCreateFnType)(void*);

class OpRunner {
public:
  virtual ~OpRunner() {}

  void *Start() {
    tid_ = (unsigned long int)pthread_self();
    return Run();
  }

protected:
  uint32_t  tid_;
  virtual void* Run() = 0;
};


class RowSpec {
public:
  RowSpec() :
      runner(NULL), key(NULL), first_cell(NULL) {
  }

  void Destroy() {
    cell_data_t *cell = first_cell;
    while (cell) {
      bytebuffer_free(cell->value);
      free(cell->hb_cell);
      cell_data_t *cur_cell = cell;
      cell = cell->next_cell;
      free(cur_cell);
    }
    first_cell = NULL;
    if (key) {
      bytebuffer_free(key);
      key = NULL;
    }
    delete this;
  }

  OpRunner *runner;

  bytebuffer key;
  struct cell_data_t_ *first_cell;
};

} /* namespace test */
} /* namespace hbase */

#endif /* HBASE_TESTS_TYPES_H_ */
