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
#include <jni.h>
#include <errno.h>

#include "hbase_macros.h"
#include "hbase_msgs.h"
#include "hbase_result.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

/**
 * Frees any resources held by the hb_result_t object.
 */
HBASE_API int32_t
hb_result_destroy(hb_result_t r) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);

  delete reinterpret_cast<Result *>(r);
  return 0;
}

/**
 * Returns a pointer and length specifying the table name associated with the
 * hb_result_t object. This buffer is valid until hb_result_free() is called.
 * Caller should not modify this buffer
 */
HBASE_API int32_t
hb_result_get_table(
    const hb_result_t r,
    char **table_ptr,
    size_t *table_length_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((table_ptr == NULL),
      Msgs::ERR_TBLPTR_NULL);
  RETURN_IF_INVALID_PARAM((table_length_ptr == NULL),
      Msgs::ERR_TBL_LENPTR_NULL);

  return reinterpret_cast<Result *>(r)->
      GetTable(table_ptr, table_length_ptr).GetCode();
}

/**
 * Returns a pointer and length specifying the name space associated with the
 * hb_result_t object. This buffer is valid until hb_result_free() is called.
 * Caller should not modify this buffer
 */
HBASE_API int32_t
hb_result_get_namespace(
    const hb_result_t result,
    char **name_space,
    size_t *name_space_length_ptr) {
  return ENOSYS;
}

/**
 * Populates the provided hb_cell_t structure with the most recent value of the
 * given column. The buffers pointed by hb_cell_t structure can be freed by
 * calling hb_cell_free().
 *
 * @returns 0 if operation succeeds
 * @returns ENOENT if a matching cell is not found
 */
HBASE_API int32_t
hb_result_get_cell(
    const hb_result_t r,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len,
    hb_cell_t *cell_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((family == NULL),
      Msgs::ERR_FAMILY_NULL);
  RETURN_IF_INVALID_PARAM((family_len <= 0),
      Msgs::ERR_FAMILY_LEN, family_len);
  RETURN_IF_INVALID_PARAM((qualifier == NULL),
      Msgs::ERR_QUAL_NULL);
  RETURN_IF_INVALID_PARAM((qualifier_len <= 0),
      Msgs::ERR_QUAL_LEN, qualifier_len);
  RETURN_IF_INVALID_PARAM((cell_ptr == NULL),
      Msgs::ERR_CELLPTR_NULL);

  return reinterpret_cast<Result*>(r)->
      GetCell(family, family_len, qualifier,
          qualifier_len, cell_ptr).GetCode();
}

/**
 * Returns the total number of cells in the hb_result_t object.
 */
HBASE_API int32_t
hb_result_get_cell_count(
    const hb_result_t r,
    size_t *cell_count_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((cell_count_ptr == NULL),
      Msgs::ERR_CELL_COUNTPTR_NULL);

  return reinterpret_cast<Result*>(r)->
      GetCellCount(cell_count_ptr).GetCode();
}

/**
 * Populates the provided hb_cell_t structure with the cell value at the given 0
 * based index of the result. The buffers pointed by hb_cell_t structures can be
 * freed by calling hb_cell_free().
 */
HBASE_API int32_t
hb_result_get_cell_at(
    const hb_result_t r,
    const size_t index,
    hb_cell_t *cell_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((cell_ptr == NULL),
      Msgs::ERR_CELLPTR_NULL);

  int32_t retCode = 0;
  Result *result = reinterpret_cast<Result*>(r);
  if ((retCode = result->GetRowKey(NULL, NULL).GetCode()) != 0) {
    return retCode;
  }
  if ((retCode = result->GetCellCount(NULL).GetCode()) != 0) {
    return retCode;
  }
  return result->GetCellAt(index, cell_ptr).GetCode();
}

/**
 * Populates the provided array of hb_cell_t structures with the cell values
 * of the result. The buffers pointed by hb_cell_t structures can be freed
 * by calling hb_result_destroy().
 * Calling this function multiple times for the same hb_result_t may return
 * the same buffers.
 */
HBASE_API int32_t
hb_result_get_cells(
    const hb_result_t r,
    hb_cell_t *cell_ptr[],
    size_t *num_cells) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);

  return reinterpret_cast<Result *>(r)->
      GetCells(cell_ptr, num_cells).GetCode();
}

/**
 * Frees the library allocated buffers associated with the cell. All pointers
 * of hb_cell_t object becomes invalid after this call. Application should only
 * call this function with the cells extracted from the hb_result_t object.
 * The memory occupied by the structure itself if not freed.
 */
HBASE_API int32_t
hb_cell_free(hb_cell_t *cell_ptr) {
  RETURN_IF_INVALID_PARAM((cell_ptr == NULL),
      Msgs::ERR_CELLPTR_NULL);

  if (cell_ptr->family) {
    delete[] cell_ptr->family;
    cell_ptr->family = NULL;
  }
  if (cell_ptr->qualifier) {
    delete[] cell_ptr->qualifier;
    cell_ptr->qualifier = NULL;
  }
  if (cell_ptr->value) {
    delete[] cell_ptr->value;
    cell_ptr->value = NULL;
  }
  return 0;
}

} /* extern "C" */

/**
 * Wraps a JNI object handle into @link Result class
 */
hb_result_t
Result::From(
    const jthrowable jthr,
    const jobject result,
    JNIEnv *env) {
  if (result) {
    // TODO: wrap exception if not null
    return (hb_result_t) new Result(env->NewGlobalRef(result));
  }
  return NULL;
}

Result::Result(jobject resultProxy)
: JniObject(resultProxy),
  tableName_(NULL),
  tableNameLen_(0),
  rowKey_(NULL),
  rowKeyLen_(0),
  cells_(NULL),
  cellCount_(0) {
}

Result::~Result() {
  if (tableName_ != NULL) {
    delete[] tableName_;
    tableName_ = NULL;
  }
  if (rowKey_ != NULL) {
    delete[] rowKey_;
    rowKey_ = NULL;
  }
  if (cells_ != NULL) {
    for (size_t i = 0; i < cellCount_; ++i) {
      hb_cell_free(&cells_[i]);
    }
    delete[] cells_;
    cells_ = NULL;
  }
}

Status
Result::GetTable(
    char **table,
    size_t *tableLength,
    JNIEnv *current_env) {
  if (!tableName_) {
    JNI_GET_ENV(current_env);
    JniResult result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_RESULT_PROXY, "getTable", "()[B");
    RETURN_IF_ERROR(result);
    RETURN_IF_ERROR(JniHelper::CreateByteArray(
        env, result.GetObject(),
        (byte_t **)&tableName_, &tableNameLen_));
  }
  if (table) {
    *table = tableName_;
    *tableLength = tableNameLen_;
  }
  return Status::Success;
}

Status
Result::GetRowKey(
    byte_t **rowKey,
    size_t *rowKeyLen,
    JNIEnv *current_env) {
  if (!rowKey_) {
    JNI_GET_ENV(current_env);
    JniResult result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_RESULT_PROXY, "getRowKey", "()[B");
    RETURN_IF_ERROR(result);
    RETURN_IF_ERROR(JniHelper::CreateByteArray(
        env, result.GetObject(), &rowKey_, &rowKeyLen_));
  }
  if (rowKey) {
    *rowKey = rowKey_;
    *rowKeyLen = rowKeyLen_;
  }
  return Status::Success;
}

Status
Result::GetCell(
    const byte_t *f,
    const size_t f_len,
    const byte_t *q,
    const size_t q_len,
    hb_cell_t *cell_ptr,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  GetCellCount(NULL, env);
  if (!cellCount_) {
    return Status::ENoEntry;
  }

  JniResult family = JniHelper::CreateJavaByteArray(env, f, 0, f_len);
  RETURN_IF_ERROR(family);

  JniResult qualifier = JniHelper::CreateJavaByteArray(env, q, 0, q_len);
  RETURN_IF_ERROR(qualifier);

  RETURN_IF_ERROR(GetRowKey(NULL, NULL, env));

  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "indexOf",
      "([B[B)I", family.GetObject(), qualifier.GetObject());
  RETURN_IF_ERROR(result);

  int32_t cellIndex = result.GetValue().i;
  if (cellIndex < 0) {
    return Status::ENoEntry;
  }

  return GetCellAt(cellIndex, cell_ptr, env);
}

Status
Result::GetCellCount(
    size_t *count_ptr,
    JNIEnv *current_env) {
  JniResult result;
  if (!cellCount_) {
    JNI_GET_ENV(current_env);
    result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_RESULT_PROXY, "getCellCount", "()I");
    cellCount_ = (result.ok() ? result.GetValue().i : -1);
  }
  if (count_ptr) {
    *count_ptr = cellCount_;
  }
  return result;
}

/**
 * Precondition: GetCellCount() and GetRowKey() has been called.
 */
Status
Result::GetCellAt(
    const size_t index,
    hb_cell_t *cell_ptr,
    JNIEnv *current_env) {
  // Precondition: GetCellCount()
  if (UNLIKELY(index >= cellCount_)) {
    return Status::ERange;
  }

  // Precondition: GetRowKey()
  cell_ptr->row = rowKey_;
  cell_ptr->row_len = rowKeyLen_;

  JNI_GET_ENV(current_env);

  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "getFamily", "(I)[B", index);
  RETURN_IF_ERROR(result);
  RETURN_IF_ERROR(JniHelper::CreateByteArray(
          env, result.GetObject(), &cell_ptr->family, &cell_ptr->family_len));

  result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "getQualifier", "(I)[B", index);
  RETURN_IF_ERROR(result);
  RETURN_IF_ERROR(JniHelper::CreateByteArray(
          env, result.GetObject(), &cell_ptr->qualifier, &cell_ptr->qualifier_len));

  result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "getValue", "(I)[B", index);
  RETURN_IF_ERROR(result);
  RETURN_IF_ERROR(JniHelper::CreateByteArray(
          env, result.GetObject(), &cell_ptr->value, &cell_ptr->value_len));

  result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "getTS", "(I)J", index);
  RETURN_IF_ERROR(result);
  cell_ptr->ts = result.GetValue().j;

  return result;
}

Status
Result::GetCells(
    hb_cell_t **cell_ptr,
    size_t *num_cells,
    JNIEnv *current_env) {
  Status status = Status::Success;
  if (!cells_) {
    JNI_GET_ENV(current_env);

    status = GetRowKey(NULL, NULL, env);
    RETURN_IF_ERROR(status);

    status = GetCellCount(NULL, env);
    RETURN_IF_ERROR(status);

    cells_ = new hb_cell_t[cellCount_];
    if (UNLIKELY(cells_ == NULL)) {
      return Status::ENoMem;
    }
    for (size_t i = 0; i < cellCount_; ++i) {
      GetCellAt(i, &cells_[i], env);
    }
  }

  *cell_ptr = cells_;
  *num_cells = cellCount_;
  return status;
}

} /* namespace hbase */
