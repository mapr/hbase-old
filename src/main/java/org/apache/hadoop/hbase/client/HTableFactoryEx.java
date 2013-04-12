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
import org.apache.hadoop.hbase.client.mapr.TableMappingRulesFactory;

/**
 * Factory for creating HTable instances.
 *
 * @since 0.21.0
 */
public class HTableFactoryEx extends GenericHFactory<AbstractHTableInterface> 
    implements HTableInterfaceFactoryEx {
  private static final String DEFAULT_MAPR_IMPL = "com.mapr.fs.HTableImpl";
  private static final String DEFAULT_HBASE_IMPL = "org.apache.hadoop.hbase.client.HTableHBase";

  private static final HTableFactoryEx INSTANCE = new HTableFactoryEx();

  public static final HTableFactoryEx get() {
    return INSTANCE;
  }

  @Override
  public AbstractHTableInterface createHTableInterface(Configuration config,
      byte[] tableName) {
    try {
      return getImplementorInstance(config, tableName,
          new Object[] {config, tableName},
          new Class[] {Configuration.class, byte[].class});
    } catch (Exception e) {
      // FIXME Do proper error handling
      throw new RuntimeException(e);
    }
  }

  @Override
  public AbstractHTableInterface createHTableInterface(Configuration conf,
      byte[] tableName, ExecutorService pool) {
      try {
        return getImplementorInstance(conf, tableName,
            new Object[] {conf, tableName, pool},
            new Class[] {Configuration.class, byte[].class, ExecutorService.class});
      } catch (Exception e) {
        // FIXME Do proper error handling
        throw new RuntimeException(e);
      }
  }

  @Override
  public AbstractHTableInterface createHTableInterface(byte[] tableName,
      HConnection connection, ExecutorService pool) {
    try {
      return getImplementorInstance(connection.getConfiguration(), tableName,
          new Object[] {tableName, connection, pool},
          new Class[] {byte[].class, HConnection.class, ExecutorService.class});
    } catch (Exception e) {
      // FIXME Do proper error handling
      throw new RuntimeException(e);
    }
  }

  @Override
  public void releaseHTableInterface(AbstractHTableInterface table) throws IOException {
    table.close();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends AbstractHTableInterface> getImplementingClass(
      Configuration conf, String tableName) throws IOException {
    String engine = TableMappingRulesFactory.create(conf).isMapRTable(tableName)
          ? conf.get("htable.impl.mapr", DEFAULT_MAPR_IMPL)
          : conf.get("htable.impl.hbase", DEFAULT_HBASE_IMPL);

    try {
      return (Class<? extends AbstractHTableInterface>) conf.getClassByName(engine);
    }
    catch (Exception e) {
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
  }
}
