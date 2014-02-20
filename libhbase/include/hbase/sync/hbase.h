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

#ifndef LIBHBASE_SYNC_H_
#define LIBHBASE_SYNC_H_

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
 * This header file describes the synchronous APIs and data structures of a C
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
 * 3. Its applications responsibility to free up all the backing data buffers.
 *    Asynchronous APIs do not make a copy of data buffers hence the ownership
 *    of these buffers is temporarily transfered to the library until the
 *    callback is triggered. Application should not modify these buffers while
 *    they are in flight.
 *
 * 4. No explicit batching is supported for asynchronous APIs.
 *  ...
 */

/*===========================================================================*
 *                                 Admin APIs                                *
 *===========================================================================*/

/**
 * Checks if a table exists.
 * @returns 0 on if the table exist, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_exists(
    const hb_admin_t admin,   /* [in] HBaseClient handle */
    const char *name_space,   /* [in] Null terminated namespace, set to NULL
                               *   for default namespace and for 0.94 version */
    const char *table_name);  /* [in] Null terminated table name */

/**
 * Checks if a table is enabled.
 * @returns 0 on if the table enabled, HBASE_TABLE_DISABLED if the table
 * is disabled or an error code if an error occurs.
 */
HBASE_API int32_t
hb_admin_table_enabled(
    const hb_admin_t admin,   /* [in] HBaseClient handle */
    const char *name_space,   /* [in] Null terminated namespace, set to NULL
                               *   for default namespace and for 0.94 version */
    const char *table_name);  /* [in] Null terminated table name */

/**
 * Creates an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_create(
    const hb_admin_t admin,         /* [in] HBaseClient handle */
    const char *name_space,         /* [in] Null terminated namespace, set to NULL
                                     *   for default namespace and for 0.94 version */
    const char *table_name,         /* [in] Null terminated table name */
    const hb_columndesc families[], /* [in] Array of Null terminated family names */
    const size_t num_families);     /* [in] Number of families */

/**
 * Disable an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_disable(
    const hb_admin_t admin,  /* [in] HBaseClient handle */
    const char *name_space,  /* [in] Null terminated namespace, set to NULL
                              *   for default namespace and for 0.94 version */
    const char *table_name); /* [in] Null terminated table name */

/**
 * Enable an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_enable(
    const hb_admin_t admin,  /* [in] HBaseClient handle */
    const char *name_space,  /* [in] Null terminated namespace, set to NULL
                              *   for default namespace and for 0.94 version */
    const char *table_name); /* [in] Null terminated table name */

/**
 * Deletes an HBase table, disables the table if not already disabled.
 * Returns 0 on success, and error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_delete(
    const hb_admin_t admin , /* [in] HBaseClient handle */
    const char *name_space,  /* [in] Null terminated namespace, set to NULL
                              *   for default namespace and for 0.94 version */
    const char *table_name); /* [in] Null terminated table name */

/*===========================================================================*
 *                                 Data APIs                                 *
 *===========================================================================*/
/**
 * @NotYetImplemented
 *
 * Creates a connection to an HBase table. The returned handle is NOT
 * thread safe.
 *
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_table_open(
    hb_connection_t conn,   /* [in] hb_connection_t handle */
    const char *name_space, /* [in] NULL terminated HBase name space. If set to NULL,
                             *   "default" namespace is assumed. Ignored for 0.94.x */
    const char *table_name, /* [in] NULL terminated table name */
    hb_table_t *table);     /* [out] pointer to an hb_table_t handle */

/**
 * @NotYetImplemented
 *
 * Close table connection and release resources.
 *
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_table_close(hb_table_t table);

/**
 * @NotYetImplemented
 *
 * Send the mutations to the server in a batch synchronously.
 *
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_table_send_mutations(
    hb_table_t table,          /* [in] hb_table_t handle */
    hb_mutation_t mutations[], /* [in] array of hb_mutation_t handles */
    int32_t num_mutations);    /* [in] number of mutations in the array */

/**
 * @NotYetImplemented
 *
 * Send the puts to the server asynchronously.
 *
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_table_send_puts(
    hb_table_t table,  /* [in] hb_table_t handle */
    hb_put_t puts[],   /* [in] array of hb_put_t handles */
    int32_t num_puts); /* [in] number of puts in the array */

/**
 * @NotYetImplemented
 *
 * Executes all the buffered Puts for the specified table.
 */
HBASE_API int32_t
hb_table_flush_puts(
    hb_table_t table); /* [in] hb_table_t handle */

/**
 * @NotYetImplemented
 *
 * Fetch 'num_gets' results, one for each get in 'gets' from the server.
 *
 * Caller needs to provide arrays of hb_get_t and hb_result_t handles each
 * of size 'num_gets'. Upon return the elements of hb_result_t array are set
 * to a valid hb_result_t if a result could be fetched for the corresponding
 * hb_get_t in hb_get_t array or set to NULL otherwise.
 *
 * @returns 0 on success (including NULL results), non-zero otherwise.
 */
HBASE_API int32_t
hb_table_getrows(
    hb_table_t table,       /* [in] hb_table_t handle */
    hb_get_t gets[],        /* [in] array of hb_get_t handles */
    int32_t num_gets,       /* [in] number of hb_get_t handles in the array */
    hb_result_t results[]); /* [out] array of hb_result_t handles */

/**
 * @NotYetImplemented
 *
 * Fetch up to next 'num_rows' rows for the hb_scanner
 *
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_scanner_getnext(
    hb_scanner_t scanner,  /* [in] hb_scanner_t handle */
    hb_result_t results[], /* [in/out] array of hb_result_t handles, populated
                            *   up to the number of results actually fetched */
    int *num_rows);        /* [in/out] number of results to fetch. Upon return set
                            *   to number actually fetched */

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* LIBHBASE_SYNC_H_ */
