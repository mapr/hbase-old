/**
 * Copyright 2010 The Apache Software Foundation
 *
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
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;


/**
 * Defines methods to create new HTableInterface.
 *
 * @since 0.21.0
 */
public interface HTableInterfaceFactoryEx {

  /**
   * Creates a new AbstractHTableInterface.
   *
   * @param config HBaseConfiguration instance.
   * @param tableName name of the HBase table.
   * @return AbstractHTableInterface instance.
   */
  AbstractHTableInterface createHTableInterface(Configuration config, byte[] tableName);

  /**
   * Creates a new AbstractHTableInterface.
   *
   * @param config HBaseConfiguration instance.
   * @param tableName name of the HBase table.
   * @param pool ExecutorService to be used.
   * @return AbstractHTableInterface instance.
   */
  AbstractHTableInterface createHTableInterface(Configuration config, 
      final byte[] tableName, final ExecutorService pool);

  /**
   * Creates a new AbstractHTableInterface.
   * 
   * @param tableName Name of the table.
   * @param connection HConnection to be used.
   * @param pool ExecutorService to be used.
   */
  AbstractHTableInterface createHTableInterface(final byte[] tableName, 
      final HConnection connection, final ExecutorService pool);

  /**
   * Release the HTable resource represented by the table.
   * @param table
   */
  void releaseHTableInterface(final AbstractHTableInterface table) throws IOException;
}
