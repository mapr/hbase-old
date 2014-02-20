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

#ifndef LIBHBASE_RESULT_H_
#define LIBHBASE_RESULT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * Returns a pointer and length specifying the table name associated with the
 * hb_result_t object. This buffer is valid until hb_result_free() is called.
 * Caller should not modify this buffer
 */
HBASE_API int32_t
hb_result_get_table(const hb_result_t result,
    char **table, size_t *table_length);

/**
 * @NotYetImplemented
 *
 * HBase 0.96 or later: Returns a pointer and length specifying the name space
 * associated with the hb_result_t object. This buffer is valid until
 * hb_result_free() is called.
 * Caller should not modify this buffer
 */
HBASE_API int32_t
hb_result_get_namespace(const hb_result_t result,
    char **name_space, size_t *name_space_length);

/**
 * Populates the provided hb_cell_t structure with the most recent value of the
 * given column. The buffers pointed by hb_cell_t structure can be freed by
 * calling hb_cell_free().
 *
 * @returns 0 if operation succeeds
 * @returns ENOENT if a matching cell is not found
 */
HBASE_API int32_t
hb_result_get_cell(const hb_result_t result,
    const byte_t *family, const size_t family_len,
    const byte_t *qualifier, const size_t qualifier_len,
    hb_cell_t *cell_ptr);

/**
 * Returns the total number of cells in the hb_result_t object.
 */
HBASE_API int32_t
hb_result_get_cell_count(const hb_result_t result, size_t *cell_count);

/**
 * Populates the provided hb_cell_t structure with the cell value at the given 0
 * based index of the result. The buffers pointed by hb_cell_t structures can be
 * freed by calling hb_cell_free().
 */
HBASE_API int32_t
hb_result_get_cell_at(const hb_result_t result,
    const size_t index, hb_cell_t *cell_ptr);

/**
 * Populates the provided array of hb_cell_t structures with the cell values
 * of the result. The buffers pointed by hb_cell_t structures can be freed
 * by calling hb_result_destroy().
 * Calling this function multiple times for the same hb_result_t may return
 * the same buffers.
 */
HBASE_API int32_t
hb_result_get_cells(const hb_result_t result,
    hb_cell_t *cell_ptr[], size_t *num_cells);

/**
 * Frees the library allocated buffers associated with the cell. All pointers
 * of hb_cell_t object becomes invalid after this call. Application should only
 * call this function with the cells extracted from the hb_result_t object.
 * The memory occupied by the structure itself if not freed.
 */
HBASE_API int32_t
hb_cell_free(hb_cell_t *cell_ptr);

/**
 * Frees any resources held by the hb_result_t object.
 */
HBASE_API int32_t
hb_result_destroy(hb_result_t result);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif  /* LIBHBASE_RESULT_H_*/
