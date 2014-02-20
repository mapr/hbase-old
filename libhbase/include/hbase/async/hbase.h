/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef LIBHBASE_ASYNC_H_
#define LIBHBASE_ASYNC_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "../admin.h"
#include "../client.h"
#include "../coldesc.h"
#include "../connection.h"
#include "../get.h"
#include "../log.h"
#include "../mutations.h"
#include "../result.h"
#include "../scanner.h"
#include "../types.h"

/*
 * This header file describes the a-synchronous APIs and data structures of a C
 * client for Apache HBase.
 *
 * Following conventions are used.
 *
 * 1. All data types are prefixed with the 'hb_'.
 *
 * 2. All exported functions are annotated with HBASE_API, prefixed with 'hb_'
 *    and named using the following convention:
 *    'hb_<subject>_<operation>_[<object>|<property>]'.
 *
 * 3. All asynchronous APIs take a callback which is triggered when the request
 *    completes. This callback can be triggered in the callers thread or another
 *    thread. To avoid any potential deadlock/starvation, applications should not
 *    block in the callback routine.
 *
 * 4. All callbacks take a void pointer for the user to supply their own data
 *    which is passed when callback is triggered.
 *
 * 4. Its applications responsibility to free up all the backing data buffers.
 *    Asynchronous APIs do not make a copy of data buffers hence the ownership
 *    of these buffers is temporarily transfered to the library until the
 *    callback is triggered. Application should not modify these buffers while
 *    they are in flight.
 *
 * 5. No explicit batching is supported for asynchronous APIs.
 *  ...
 */

/**
 * Queue a mutation to go out. These mutations will not be performed
 * atomically and can be batched in a non-deterministic way on either
 * the server or the client side.
 *
 * Any buffer attached to the the mutation objects must not be altered
 * until the callback has been triggered.
 */
HBASE_API int32_t
hb_mutation_send(
    hb_client_t client,
    hb_mutation_t mutation,
    hb_mutation_cb cb,
    void *extra);

/**
 * Queues the get request. Callback specified by cb will be called
 * on completion. Any buffer(s) attached to the get object can be
 * reclaimed only after the callback is received.
 */
HBASE_API int32_t
hb_get_send(
    hb_client_t client,
    hb_get_t get,
    hb_get_cb cb,
    void *extra);

/**
 * @NotYetImplemented
 *
 * Request the next set of results from the server. You can set the maximum
 * number of rows returned by this call using hb_scanner_set_num_max_rows().
 */
HBASE_API int32_t
hb_scanner_next(
    hb_scanner_t scanner,
    hb_scanner_cb cb,
    void *extra);

/**
 * @NotYetImplemented
 *
 * Close the scanner releasing any local and server side resources held.
 * The call back is fired just before the scanner's memory is freed.
 */
HBASE_API int32_t
hb_scanner_destroy(
    hb_scanner_t scanner,
    hb_scanner_destroy_cb cb,
    void *extra);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* LIBHBASE_ASYNC_H_ */
