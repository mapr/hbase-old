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
#ifndef FLUSH_OPS_H_
#define FLUSH_OPS_H_

#include <hbase/hbase.h>

#include "stats_keeper.h"

namespace hbase {
namespace test {

class Flusher : public TaskRunner {
public:
  Flusher(
      hb_client_t client,
      uint64_t flushBatchSize,
      StatKeeper *statKeeper) :
    client_(client),
    flushBatchSize_(flushBatchSize),
    statKeeper_(statKeeper) {
  }

  void* Run();

private:
  const hb_client_t client_;

  const uint64_t flushBatchSize_;

  StatKeeper *statKeeper_;
};

} /* namespace test */
} /* namespace hbase */

#endif /* FLUSH_OPS_H_ */
