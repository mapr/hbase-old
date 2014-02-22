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
#ifndef HBASE_JNI_IMPL_RESULT_H_
#define HBASE_JNI_IMPL_RESULT_H_

#include <jni.h>

#include <hbase/types.h>

#include "jnihelper.h"
#include "status.h"

namespace hbase {

class Result : public JniObject {
public:
  Result(jobject resultProxy);

  ~Result();

  Status GetTable(char **table, size_t *tableLength, JNIEnv *current_env=NULL);

  Status GetRowKey(byte_t **rowKey, size_t *rowKeyLength, JNIEnv *current_env=NULL);

  Status GetCell(const byte_t *family, const size_t family_len, const byte_t *qualifier,
      const size_t qualifier_len, hb_cell_t *cell_ptr, JNIEnv *current_env=NULL);

  Status GetCellCount(size_t *countPtr, JNIEnv *current_env=NULL);

  Status GetCellAt(const size_t index, hb_cell_t *cell_ptr, JNIEnv *current_env=NULL);

  Status GetCells(hb_cell_t *cell_ptr[], size_t *num_cells, JNIEnv *current_env=NULL);

  static hb_result_t From(jthrowable jthr, jobject result, JNIEnv *env);

private:
  char      *tableName_;
  size_t    tableNameLen_;

  byte_t    *rowKey_;
  size_t    rowKeyLen_;

  hb_cell_t *cells_;
  size_t    cellCount_;
};

} /* namespace hbase */

#endif /* HBASE_JNI_IMPL_RESULT_H_ */
