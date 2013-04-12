/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.mapr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface TableMappingRulesInterface {
  public static final String HBASE_AVAILABLE = "hbase.available";

  public static final String MAPRFS_PREFIX = "maprfs://";
  public static final String HBASE_PREFIX = "hbase://";

  /**
   * Describe the type of cluster based on the running services.
   */
  public enum ClusterType {
    /**
     * The cluster runs only HBase service (pre 3.0)
     */
    HBASE_ONLY,
    /**
     * HBase is not installed in the cluster.
     */
    MAPR_ONLY,
    /**
     * The cluster runs both type of DB services.
     */
    HBASE_MAPR
  }

  /**
   * Returns one of the possible {@link ClusterType}
   * @return
   */
  public ClusterType getClusterType();

  /**
   * @return <code>true</code> if Running with MapR DB and either "db.engine.default"
   * is set to "mapr" or one of the table mapping rule maps "*" to some path
   */
  public boolean isMapRDefault();

  /**
   * @return the mapping to "*" in the namespace mapping if configured, 
   * otherwise current working directory.
   * @throws IOException 
   */
  public Path getDefaultTablePath() throws IOException;

  /**
   * Tests if <code>tableName</code> should be treated as MapR table
   *
   * @param tableName
   *
   * @return  <code>true</code> if the table is determined to be a MapR table.
   * @throws IOException
   * @throws IllegalArgumentException If the passed {@code tableName} is null
   */
  public boolean isMapRTable(byte[] tableName) throws IOException;
  public boolean isMapRTable(String tableName) throws IOException;

  /**
   *  Returns translated path according to the configured mapping
   *
   *  @param  tableName Absolute or relative table name
   *
   *  @return Translated absolute path if the table is a MapR table,
   *          <code>null</code> otherwise
   */
  public Path getMaprTablePath(byte[] tableName) throws IOException;
  public Path getMaprTablePath(String tableName) throws IOException;

  /**
   * Check if the passed , "tableName", is legal table name.
   * @return Returns passed <code>tableName</code> param
   * @throws NullPointerException If passed <code>tableName</code> is null
   * @throws IllegalArgumentException if passed a tableName is an HBase table
   * and is made of other than 'word' characters or underscores: i.e.
   * <code>[a-zA-Z_0-9].
   */
  public byte [] isLegalTableName(final byte [] tableName);
  public String isLegalTableName(final String tableName);
}
