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

#ifndef LIBHBASE_SCANNER_H_
#define LIBHBASE_SCANNER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * @NotYetImplemented
 *
 * Creates a client side row scanner. The returned scanner is not thread safe.
 * No RPC will be invoked until the call to fetch the next set of rows is made.
 * You can set the various attributes of this scanner until that point.
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t hb_scanner_create(
    hb_client_t client,         /* [in] */
    hb_scanner_t *scanner_ptr); /* [out] */

/**
 * @NotYetImplemented
 *
 * Set the table name for the scanner
 */
HBASE_API int32_t
hb_scanner_set_table(
    hb_scanner_t scanner,
    char *table,
    size_t table_length);

/**
 * @NotYetImplemented
 *
 * Set the name space for the scanner (0.96 and above)
 */
HBASE_API int32_t
hb_scanner_set_namespace(
    hb_scanner_t scanner,
    char *name_space,
    size_t name_space_length);

/**
 * @NotYetImplemented
 *
 * Specifies the start row key for this scanner (inclusive).
 */
HBASE_API int32_t
hb_scanner_set_start_row(
    hb_scanner_t scanner,
    byte_t *start_row,
    size_t start_row_length);

/**
 * @NotYetImplemented
 *
 * Specifies the end row key for this scanner (exclusive).
 */
HBASE_API int32_t
hb_scanner_set_end_row(
    hb_scanner_t scanner,
    byte_t *end_row,
    size_t end_row_length);

/**
 * @NotYetImplemented
 *
 * Sets the maximum versions of a column to fetch.
 */
HBASE_API int32_t
hb_scanner_set_num_versions(
    hb_scanner_t scanner,
    int8_t num_versions);

/**
 * @NotYetImplemented
 *
 * Sets the maximum number of rows to scan per call to hb_scanner_next().
 */
HBASE_API int32_t
hb_scanner_set_num_max_rows(
    hb_scanner_t scanner,
    size_t cache_size);

#ifdef __cplusplus
}

/* extern "C" */
#endif

#endif  /* LIBHBASE_SCANNER_H_*/
