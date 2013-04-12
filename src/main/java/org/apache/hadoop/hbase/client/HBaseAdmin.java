/**
 * Copyright 2011 The Apache Software Foundation
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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.mapr.TableMappingRulesFactory;
import org.apache.hadoop.hbase.client.mapr.TableMappingRulesInterface;
import org.apache.hadoop.hbase.client.mapr.TableMappingRulesInterface.ClusterType;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest.CompactionState;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Provides an interface to manage HBase database table metadata + general
 * administrative functions.  Use HBaseAdmin to create, drop, list, enable and
 * disable tables. Use it also to add and drop table column families.
 *
 * <p>See {@link HTable} to add, update, and delete data from an individual table.
 * <p>Currently HBaseAdmin instances are not expected to be long-lived.  For
 * example, an HBaseAdmin instance will not ride over a Master restart.
 */
public class HBaseAdmin implements Abortable, Closeable {
  private static final HBaseAdminFactory adminFactory = HBaseAdminFactory.get();
  private static final Log LOG = LogFactory.getLog(HBaseAdmin.class);
  private static final AtomicBoolean balancer_ = new AtomicBoolean();

  private volatile HBaseAdminInterface apacheHBaseAdmin_ = null;
  private volatile HBaseAdminInterface maprHBaseAdmin_ = null;

  private Configuration conf_;
  private TableMappingRulesInterface tableMappingRule_;

  private volatile boolean isHbaseAvailable_ = true;
  private volatile IOException hbaseIOException_ = null;

  abstract class HBaseAdminConstructor {
    abstract HBaseAdminInterface construct();
  }
  HBaseAdminConstructor adminConstrcutor_ = null;

  /**
   * Constructor
   *
   * @param c Configuration object
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public HBaseAdmin(Configuration c)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    conf_ = c;
    try {
      tableMappingRule_ = TableMappingRulesFactory.create(conf_);
      adminConstrcutor_ = new HBaseAdminConstructor() {
        @Override
        HBaseAdminInterface construct() {
          return adminFactory.create(conf_,
            TableMappingRulesInterface.HBASE_PREFIX);
        }
      };
    } catch (IOException e) {
      throw new RuntimeException("Unable to create TableMappingRules class", e);
    }
  }

 /**
   * Constructor for externally managed HConnections.
   * This constructor fails fast if the HMaster is not running.
   * The HConnection can be re-used again in another attempt.
   * This constructor fails fast.
   *
   * @param connection The HConnection instance to use
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public HBaseAdmin(HConnection connection)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    conf_ = connection.getConfiguration();
    try {
      final HConnection conn = connection;
      tableMappingRule_ = TableMappingRulesFactory.create(conf_);
      adminConstrcutor_ = new HBaseAdminConstructor() {
        @Override
        HBaseAdminInterface construct() {
          return adminFactory.create(conn,
            TableMappingRulesInterface.HBASE_PREFIX);
        }
      };
    } catch (IOException e) {
      throw new RuntimeException("Unable to create TableMappingRules class", e);
    }
  }

  @Override
  public void abort(String why, Throwable e) {
    try {
      if (getApacheHBaseAdmin(false) != null) {
        getApacheHBaseAdmin().abort(why, e);
      }
    } catch (IOException e1) {
      LOG.warn("HBaseAdmin.abort: " + e1.getMessage());
    }
  }

  @Override
  public boolean isAborted(){
    try {
      return getApacheHBaseAdmin(false) == null || getApacheHBaseAdmin().isAborted();
    } catch (IOException e) {
      LOG.warn("HBaseAdmin.isAborted: " + e.getMessage());
    }
    return true;
  }

  /** @return HConnection used by this object. */
  public HConnection getConnection() {
    try {
      return (getApacheHBaseAdmin(false) != null) ? getApacheHBaseAdmin().getConnection() : null;
    } catch (IOException e) {
      LOG.warn("HBaseAdmin.getConnection: " + e.getMessage());
    }
    return null;
  }

  /**
   * Get a connection to the currently set master.
   * @return proxy connection to master server for this instance
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   * @deprecated  Master is an implementation detail for HBaseAdmin.
   * Deprecated in HBase 0.94
   */
  @Deprecated
  public HMasterInterface getMaster()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    try {
      return (getApacheHBaseAdmin(false) != null) ? getApacheHBaseAdmin().getMaster() : null;
    } catch (IOException e) {
      LOG.warn("HBaseAdmin.getMaster: " + e.getMessage());
    }
    return null;
  }

  /** @return - true if the master server is running
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException */
  public boolean isMasterRunning()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    try {
      return (getApacheHBaseAdmin() == null) || getApacheHBaseAdmin().isMasterRunning();
    } catch (IOException e) {
      LOG.warn("HBaseAdmin.isMasterRunning: " + e.getMessage());
    }
    return false;
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public boolean tableExists(final String tableName)
  throws IOException {
    return getAdmin(tableName).tableExists(tableName);
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public boolean tableExists(final byte [] tableName)
  throws IOException {
    return tableExists(Bytes.toString(tableName));
  }

  /**
   * List all the userspace tables.  In other words, scan the META table.
   *
   * If we wanted this to be really fast, we could implement a special
   * catalog table that just contains table names and their descriptors.
   * Right now, it only exists as part of the META table's region info.
   *
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor[] listTables() throws IOException {
    return (tableMappingRule_.isMapRDefault()) || (getApacheHBaseAdmin(false) == null)
        ? getMaprHBaseAdmin().listTables() 
            : getApacheHBaseAdmin().listTables();
  }

  /**
   * List all the userspace tables matching the given pattern.
   *
   * @param pattern The compiled regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTables()
   */
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    if (tableMappingRule_.isMapRTable(pattern.pattern()) ||
        getApacheHBaseAdmin(false) == null) {
      return getMaprHBaseAdmin().listTables(pattern);
    }
    else {
      if (getApacheHBaseAdmin(false) != null) {
        return getApacheHBaseAdmin().listTables(pattern);
      }
      return new HTableDescriptor[0];
    }
  }

  /**
   * List all the userspace tables matching the given regular expression.
   *
   * @param regex The regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTables(java.util.regex.Pattern)
   */
  public HTableDescriptor[] listTables(String regex) throws IOException {
    if (tableMappingRule_.isMapRTable(regex) ||
          getApacheHBaseAdmin(false) == null) {
      return getMaprHBaseAdmin().listTables(regex);
    }
    else {
      if (getApacheHBaseAdmin(false) != null) {
        return getApacheHBaseAdmin().listTables(regex);
      }
      return new HTableDescriptor[0];
    }
  }


  /**
   * Method for getting the tableDescriptor
   * @param tableName as a byte []
   * @return the tableDescriptor
   * @throws TableNotFoundException
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final byte [] tableName)
  throws TableNotFoundException, IOException {
      return getAdmin(tableName).getTableDescriptor(tableName);
  }

  /**
   * Creates a new table.
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    getAdmin(desc.getAlias()).createTable(desc);
  }

  /**
   * Creates a new table with the specified number of regions.  The start key
   * specified will become the end key of the first region of the table, and
   * the end key specified will become the start key of the last region of the
   * table (the first region has a null start key and the last region has a
   * null end key).
   *
   * BigInteger math will be used to divide the key range specified into
   * enough segments to make the required number of total regions.
   *
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   * @param startKey beginning of key range
   * @param endKey end of key range
   * @param numRegions the total number of regions to create
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [] startKey,
      byte [] endKey, int numRegions)
  throws IOException {
    getAdmin(desc.getAlias()).createTable(desc, startKey, endKey, numRegions);
  }

  /**
   * Creates a new table with an initial set of empty regions defined by the
   * specified split keys.  The total number of regions created will be the
   * number of split keys plus one. Synchronous operation.
   * Note : Avoid passing empty split key.
   *
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   *
   * @throws IllegalArgumentException if the table name is reserved, if the split keys
   * are repeated and if the split key has empty byte array.
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(final HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    getAdmin(desc.getAlias()).createTable(desc, splitKeys);
  }

  /**
   * Creates a new table but does not block and wait for it to come online.
   * Asynchronous operation.  To check if the table exists, use
   * {@link: #isTableAvailable} -- it is not safe to create an HTable
   * instance to this table before it is available.
   * Note : Avoid passing empty split key.
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException Bad table name, if the split keys
   * are repeated and if the split key has empty byte array.
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTableAsync(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    getAdmin(desc.getAlias()).createTableAsync(desc, splitKeys);
  }

  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final String tableName) throws IOException {
    getAdmin(tableName).deleteTable(tableName);
  }

  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final byte [] tableName) throws IOException {
    getAdmin(tableName).deleteTable(tableName);
  }

  /**
   * Deletes tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.lang.String)} and
   * {@link #deleteTable(byte[])}
   *
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   * @see #deleteTables(java.util.regex.Pattern)
   * @see #deleteTable(java.lang.String)
   */
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    return getAdmin(regex).deleteTables(regex);
  }

  /**
   * Delete tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #deleteTable(byte[])}
   *
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   */
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    return getAdmin(pattern.pattern()).deleteTables(pattern);
  }


  public void enableTable(final String tableName)
  throws IOException {
    getAdmin(tableName).enableTable(tableName);
  }

  /**
   * Enable a table.  May timeout.  Use {@link #enableTableAsync(byte[])}
   * and {@link #isTableEnabled(byte[])} instead.
   * The table has to be in disabled state for it to be enabled.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   * There could be couple types of IOException
   * TableNotFoundException means the table doesn't exist.
   * TableNotDisabledException means the table isn't in disabled state.
   * @see #isTableEnabled(byte[])
   * @see #disableTable(byte[])
   * @see #enableTableAsync(byte[])
   */
  public void enableTable(final byte [] tableName)
  throws IOException {
    getAdmin(tableName).enableTable(tableName);
  }

  public void enableTableAsync(final String tableName)
  throws IOException {
    getAdmin(tableName).enableTableAsync(tableName);
  }

  /**
   * Brings a table on-line (enables it).  Method returns immediately though
   * enable of table may take some time to complete, especially if the table
   * is large (All regions are opened as part of enabling process).  Check
   * {@link #isTableEnabled(byte[])} to learn when table is fully online.  If
   * table is taking too long to online, check server logs.
   * @param tableName
   * @throws IOException
   * @since 0.90.0
   */
  public void enableTableAsync(final byte [] tableName)
  throws IOException {
    getAdmin(tableName).enableTableAsync(tableName);
  }

  /**
   * Enable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.lang.String)} and
   * {@link #enableTable(byte[])}
   *
   * @param regex The regular expression to match table names against
   * @throws IOException
   * @see #enableTables(java.util.regex.Pattern)
   * @see #enableTable(java.lang.String)
   */
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    return getAdmin(regex).enableTables(regex);
  }

  /**
   * Enable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #enableTable(byte[])}
   *
   * @param pattern The pattern to match table names against
   * @throws IOException
   */
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    return getAdmin(pattern.pattern()).enableTables(pattern);
  }

  public void disableTableAsync(final String tableName) throws IOException {
    getAdmin(tableName).disableTableAsync(tableName);
  }

  /**
   * Starts the disable of a table.  If it is being served, the master
   * will tell the servers to stop serving it.  This method returns immediately.
   * The disable of a table can take some time if the table is large (all
   * regions are closed as part of table disable operation).
   * Call {@link #isTableDisabled(byte[])} to check for when disable completes.
   * If table is taking too long to online, check server logs.
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   * @see #isTableDisabled(byte[])
   * @see #isTableEnabled(byte[])
   * @since 0.90.0
   */
  public void disableTableAsync(final byte [] tableName) throws IOException {
    getAdmin(tableName).disableTableAsync(tableName);
  }

  public void disableTable(final String tableName)
  throws IOException {
    getAdmin(tableName).disableTable(tableName);
  }

  /**
   * Disable table and wait on completion.  May timeout eventually.  Use
   * {@link #disableTableAsync(byte[])} and {@link #isTableDisabled(String)}
   * instead.
   * The table has to be in enabled state for it to be disabled.
   * @param tableName
   * @throws IOException
   * There could be couple types of IOException
   * TableNotFoundException means the table doesn't exist.
   * TableNotEnabledException means the table isn't in enabled state.
   */
  public void disableTable(final byte [] tableName)
  throws IOException {
    getAdmin(tableName).disableTable(tableName);
  }

  /**
   * Disable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.lang.String)} and
   * {@link #disableTable(byte[])}
   *
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws IOException
   * @see #disableTables(java.util.regex.Pattern)
   * @see #disableTable(java.lang.String)
   */
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    return getAdmin(regex).disableTables(regex);
  }

  /**
   * Disable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #disableTable(byte[])}
   *
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws IOException
   */
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    return getAdmin(pattern.pattern()).disableTables(pattern);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(String tableName) throws IOException {
    return getAdmin(tableName).isTableEnabled(tableName);
  }
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return getAdmin(tableName).isTableEnabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(final String tableName) throws IOException {
    return getAdmin(tableName).isTableDisabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return getAdmin(tableName).isTableDisabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return getAdmin(tableName).isTableAvailable(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return getAdmin(tableName).isTableAvailable(tableName);
  }

  /**
   * Get the status of alter command - indicates how many regions have received
   * the updated schema Asynchronous operation.
   *
   * @param tableName
   *          name of the table to get the status of
   * @return Pair indicating the number of regions updated Pair.getFirst() is the
   *         regions that are yet to be updated Pair.getSecond() is the total number
   *         of regions of the table
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public Pair<Integer, Integer> getAlterStatus(final byte[] tableName)
  throws IOException {
    return getAdmin(tableName).getAlterStatus(tableName);
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final String tableName, HColumnDescriptor column)
  throws IOException {
    column.validate();
    getAdmin(tableName).addColumn(tableName, column);
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException {
    column.validate();
    getAdmin(tableName).addColumn(tableName, column);
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final String tableName, final String columnName)
  throws IOException {
    getAdmin(tableName).deleteColumn(tableName, columnName);
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final byte [] tableName, final byte [] columnName)
  throws IOException {
    getAdmin(tableName).deleteColumn(tableName, columnName);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final String tableName, HColumnDescriptor descriptor)
      throws IOException {
    descriptor.validate();
    getAdmin(tableName).modifyColumn(tableName, descriptor);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    descriptor.validate();
    getAdmin(tableName).modifyColumn(tableName, descriptor);
  }

  /**
   * Close a region. For expert-admins.  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param regionname region name to close
   * @param serverName If supplied, we'll use this location rather than
   * the one currently in <code>.META.</code>
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final String regionname, final String serverName)
  throws IOException {
    getAdmin(regionname).closeRegion(regionname, serverName);
  }

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param regionname region name to close
   * @param serverName The servername of the regionserver.  If passed null we
   * will use servername found in the .META. table. A server name
   * is made of host, port and startcode.  Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final byte [] regionname, final String serverName)
  throws IOException {
    getAdmin(regionname).closeRegion(regionname, serverName);
  }

  /**
   * For expert-admins. Runs close on the regionserver. Closes a region based on
   * the encoded region name. The region server name is mandatory. If the
   * servername is provided then based on the online regions in the specified
   * regionserver the specified region will be closed. The master will not be
   * informed of the close. Note that the regionname is the encoded regionname.
   *
   * @param encodedRegionName
   *          The encoded region name; i.e. the hash that makes up the region
   *          name suffix: e.g. if regionname is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>
   *          , then the encoded region name is:
   *          <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param serverName
   *          The servername of the regionserver. A server name is made of host,
   *          port and startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return true if the region was closed, false if not.
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean closeRegionWithEncodedRegionName(final String encodedRegionName,
      final String serverName) throws IOException {
    if (!HRegionInfo.isEncodedName(encodedRegionName)) {
      return getMaprHBaseAdmin().closeRegionWithEncodedRegionName(
          encodedRegionName, serverName);
    }
    else {
      return getApacheHBaseAdmin().closeRegionWithEncodedRegionName(
          encodedRegionName, serverName);
    }
  }

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param sn
   * @param hri
   * @throws IOException
   */
  public void closeRegion(final ServerName sn, final HRegionInfo hri)
  throws IOException {
    getAdmin(hri.getTableName()).closeRegion(sn, hri);
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).flush(tableNameOrRegionName);
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).flush(tableNameOrRegionName);
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).compact(tableNameOrRegionName);
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).compact(tableNameOrRegionName);
  }
  
  /**
   * Compact a column family within a table or region.
   * Asynchronous operation.
   *
   * @param tableOrRegionName table or region to compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(String tableOrRegionName, String columnFamily)
    throws IOException,  InterruptedException {
    getAdmin(tableOrRegionName).compact(tableOrRegionName, columnFamily);
  }

  /**
   * Compact a column family within a table or region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final byte [] tableNameOrRegionName, final byte[] columnFamily)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).compact(tableNameOrRegionName, columnFamily);
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).majorCompact(tableNameOrRegionName);
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).majorCompact(tableNameOrRegionName);
  }
  
  /**
   * Major compact a column family within a table or region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final String tableNameOrRegionName,
    final String columnFamily) throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).majorCompact(tableNameOrRegionName, columnFamily);
  }

  /**
   * Major compact a column family within a table or region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final byte [] tableNameOrRegionName,
    final byte[] columnFamily) throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).compact(tableNameOrRegionName, columnFamily);
  }

  /**
   * Move the region <code>r</code> to <code>dest</code>.
   * @param encodedRegionName The encoded region name; i.e. the hash that makes
   * up the region name suffix: e.g. if regionname is
   * <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   * then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param destServerName The servername of the destination regionserver.  If
   * passed the empty byte array we'll assign to a random server.  A server name
   * is made of host, port and startcode.  Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>
   * @throws UnknownRegionException Thrown if we can't find a region named
   * <code>encodedRegionName</code>
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   */
  public void move(final byte [] encodedRegionName, final byte [] destServerName)
  throws UnknownRegionException, MasterNotRunningException, ZooKeeperConnectionException {
    try {
      if (!HRegionInfo.isEncodedName(encodedRegionName)) {
        getMaprHBaseAdmin().move(encodedRegionName, destServerName);
      }
      else {
        getApacheHBaseAdmin().move(encodedRegionName, destServerName);
      }
    } catch (IOException e) {
      LOG.warn("Error while moving regions: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * @param regionName
   *          Region name to assign.
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  public void assign(final byte[] regionName) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    getAdmin(regionName).assign(regionName);
  }

  /**
   * Unassign a region from current hosting regionserver.  Region will then be
   * assigned to a regionserver chosen at random.  Region could be reassigned
   * back to the same server.  Use {@link #move(byte[], byte[])} if you want
   * to control the region movement.
   * @param regionName Region to unassign. Will clear any existing RegionPlan
   * if one found.
   * @param force If true, force unassign (Will remove region from
   * regions-in-transition too if present. If results in double assignment
   * use hbck -fix to resolve. To be used by experts).
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  public void unassign(final byte [] regionName, final boolean force)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    getAdmin(regionName).unassign(regionName, force);
  }

  /**
   * Turn the load balancer on or off.
   * @param b If true, enable balancer. If false, disable balancer.
   * @return Previous balancer value
   * @deprecated use setBalancerRunning(boolean, boolean) instead
   */
  @Deprecated
  public boolean balanceSwitch(final boolean b)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    try {
      return (getApacheHBaseAdmin(false) != null)
          ? getApacheHBaseAdmin().balanceSwitch(b) : balancer_.getAndSet(b);
    } catch (IOException e) {
      LOG.warn("HBaseAdmin.balanceSwitch: " + e.getMessage());
    }
    return balancer_.getAndSet(b);
  }

  /**
   * Turn the load balancer on or off.
   * @param on If true, enable balancer. If false, disable balancer.
   * @param synchronous If true, it waits until current balance() call, if outstanding, to return.
   * @return Previous balancer value
   */
  public boolean setBalancerRunning(final boolean on, final boolean synchronous)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    try {
      return (getApacheHBaseAdmin(false) != null)
          ? getApacheHBaseAdmin().setBalancerRunning(on, synchronous)
              : balancer_.getAndSet(on);
    } catch (IOException e) {
      LOG.warn("HBaseAdmin.setBalancerRunning: " + e.getMessage());
    }
    return balancer_.getAndSet(on);
  }

  /**
   * Invoke the balancer.  Will run the balancer and if regions to move, it will
   * go ahead and do the reassignments.  Can NOT run for various reasons.  Check
   * logs.
   * @return True if balancer ran, false otherwise.
   */
  public boolean balancer()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    try {
      return (getApacheHBaseAdmin(false) == null) || getApacheHBaseAdmin().balancer();
    } catch (IOException e) {
      LOG.warn("HBaseAdmin.balancer: " + e.getMessage());
    }
    return false;
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void split(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).split(tableNameOrRegionName);
  }

  /**
   * Split a table or an individual region.  Implicitly finds an optimal split
   * point.  Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void split(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).split(tableNameOrRegionName);
  }

  public void split(final String tableNameOrRegionName,
    final String splitPoint) throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).split(tableNameOrRegionName, splitPoint);
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException interrupt exception occurred
   */
  public void split(final byte [] tableNameOrRegionName,
      final byte [] splitPoint) throws IOException, InterruptedException {
    getAdmin(tableNameOrRegionName).split(tableNameOrRegionName, splitPoint);
  }

  /**
   * Modify an existing table, more IRB friendly version.
   * Asynchronous operation.  This means that it may be a while before your
   * schema change is updated across all of the table.
   *
   * @param tableName name of table.
   * @param htd modified description of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyTable(final byte [] tableName, HTableDescriptor htd)
  throws IOException {
    getAdmin(tableName).modifyTable(tableName, htd);
  }

  /**
   * @param tableNameOrRegionName Name of a table or name of a region.
   * @param ct A {@link CatalogTracker} instance (caller of this method usually has one).
   * @return a pair of HRegionInfo and ServerName if <code>tableNameOrRegionName</code> is
   *  a verified region name (we call {@link  MetaReader#getRegion( CatalogTracker, byte[])}
   *  else null.
   * Throw an exception if <code>tableNameOrRegionName</code> is null.
   * @throws IOException
   */
  Pair<HRegionInfo, ServerName> getRegion(final byte[] tableNameOrRegionName,
      final CatalogTracker ct) throws IOException {
    return getAdmin(tableNameOrRegionName).getRegion(tableNameOrRegionName, ct);
  }

  /**
   * Shuts down the HBase cluster
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void shutdown() throws IOException {
    if (getApacheHBaseAdmin(false) != null) {
      getApacheHBaseAdmin().shutdown();
    }
  }

  /**
   * Shuts down the current HBase master only.
   * Does not shutdown the cluster.
   * @see #shutdown()
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopMaster() throws IOException {
    if (getApacheHBaseAdmin(false) != null) {
      getApacheHBaseAdmin().stopMaster();
    }
  }

  /**
   * Stop the designated regionserver
   * @param hostnamePort Hostname and port delimited by a <code>:</code> as in
   * <code>example.org:1234</code>
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopRegionServer(final String hostnamePort)
  throws IOException {
    if (getApacheHBaseAdmin(false) != null) {
      getApacheHBaseAdmin().stopRegionServer(hostnamePort);
    }
  }

  /**
   * @return cluster status
   * @throws IOException if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    return (getApacheHBaseAdmin(false) != null)
        ? getApacheHBaseAdmin().getClusterStatus() : null;
  }

  /**
   * @return Configuration used by the instance.
   */
  public Configuration getConfiguration() {
    return conf_;
  }

  /**
   * Check to see if HBase is running. Throw an exception if not.
   *
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public static void checkHBaseAvailable(Configuration conf)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    //  No-op if MapR is the default engine
    ClusterType clusterType = ClusterType.HBASE_ONLY;
    try {
      clusterType = TableMappingRulesFactory.create(conf).getClusterType();
    } catch (IOException e) { throw new RuntimeException(e);}
    if (clusterType != ClusterType.MAPR_ONLY) {
      Configuration copyOfConf = HBaseConfiguration.create(conf);
      copyOfConf.setInt("hbase.client.retries.number", 1);
      HBaseAdminImpl admin = new HBaseAdminImpl(copyOfConf);
      try {
        admin.close();
      } catch (IOException ioe) {
        LOG.info("Failed to close connection", ioe);
      }
    }
  }

  /**
   * get the regions of a given table.
   *
   * @param tableName the name of the table
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public List<HRegionInfo> getTableRegions(final byte[] tableName)
  throws IOException {
    return getAdmin(tableName).getTableRegions(tableName);
  }

  public void close() throws IOException {
    if (apacheHBaseAdmin_ != null) {
      apacheHBaseAdmin_.close();
    }
    if (maprHBaseAdmin_ != null) {
      maprHBaseAdmin_.close();
    }
  }

 /**
 * Get tableDescriptors
 * @param tableNames List of table names
 * @return HTD[] the tableDescriptor
 * @throws IOException if a remote or network exception occurs
 */
  public HTableDescriptor[] getTableDescriptors(List<String> tableNames)
  throws IOException {
    List<HTableDescriptor> list = new ArrayList<HTableDescriptor>();
    for (String table : tableNames) {
      byte[] tableName = Bytes.toBytes(table);
      if (tableMappingRule_.isMapRTable(tableName)) {
        list.add(getMaprHBaseAdmin().getTableDescriptor(tableName));
      }
      else {
        list.add(getApacheHBaseAdmin().getTableDescriptor(tableName));
      }
    }
    return list.toArray(new HTableDescriptor[list.size()]);
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * @param serverName
   *          The servername of the regionserver. A server name is made of host,
   *          port and startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.  Names are actual
   * region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws IOException if a remote or network exception occurs
   * @throws FailedLogCloseException
   */
 public synchronized  byte[][] rollHLogWriter(String serverName)
      throws IOException, FailedLogCloseException {
   if (getApacheHBaseAdmin(false) != null) {
        return getApacheHBaseAdmin().rollHLogWriter(serverName);
   }
   return null;
  }

  public String[] getMasterCoprocessors() {
    try {
      if (getApacheHBaseAdmin(false) != null) {
        return getApacheHBaseAdmin().getMasterCoprocessors();
      }
    } catch (IOException e) {
      LOG.warn("HBaseAdmin.getMasterCoprocessors: " + e.getMessage());
    }
    return null;
  }

  /**
   * Get the current compaction state of a table or region.
   * It could be in a major compaction, a minor compaction, both, or none.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   * @return the current compaction state
   */
  public CompactionState getCompactionState(final String tableNameOrRegionName)
      throws IOException, InterruptedException {
    return getAdmin(tableNameOrRegionName).getCompactionState(tableNameOrRegionName);
  }

  /**
   * Get the current compaction state of a table or region.
   * It could be in a major compaction, a minor compaction, both, or none.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   * @return the current compaction state
   */
  public CompactionState getCompactionState(final byte [] tableNameOrRegionName)
      throws IOException, InterruptedException {
    return getAdmin(tableNameOrRegionName).getCompactionState(tableNameOrRegionName);
  }

  /**
   * Creates and returns a proxy to the CoprocessorProtocol instance running in the
   * master.
   *
   * @param protocol The class or interface defining the remote protocol
   * @return A CoprocessorProtocol instance
   */
  public <T extends CoprocessorProtocol> T coprocessorProxy(
      Class<T> protocol) {
    try {
      return (T) getApacheHBaseAdmin().coprocessorProxy(protocol);
    } catch (IOException e) {
      LOG.warn("Unable to create coprocessorProxy, HBase may not be available", e);
      throw new RuntimeException(e);
    }
  }

  protected HBaseAdminInterface getMaprHBaseAdmin() {
    if (tableMappingRule_.getClusterType() != ClusterType.HBASE_ONLY) {
      HBaseAdminInterface admin = maprHBaseAdmin_;
      if (admin == null) {
        synchronized (this) {
          admin = maprHBaseAdmin_;
          if (admin == null) {
            admin = maprHBaseAdmin_ = adminFactory.create(conf_,
                TableMappingRulesInterface.MAPRFS_PREFIX);
          }
        }
      }
      return admin;
    }
    return null;
  }

  protected HBaseAdminInterface getAdmin(String tableName) throws IOException {
    return (tableMappingRule_.isMapRTable(HRegionInfo.getTableName(tableName)))
        ? getMaprHBaseAdmin()
            : getApacheHBaseAdmin();
  }

  protected HBaseAdminInterface getAdmin(byte[] tableName) throws IOException {
    return getAdmin(Bytes.toString(tableName));
  }

  protected HBaseAdminInterface getApacheHBaseAdmin() throws IOException {
    return getApacheHBaseAdmin(true);
  }

  protected HBaseAdminInterface getApacheHBaseAdmin(boolean throwException)
      throws IOException {
    HBaseAdminInterface admin = null;
    if (!isHbaseAvailable_) {
      if (throwException) {
        throw hbaseIOException_;
      }
    }
    else if (tableMappingRule_.getClusterType() != ClusterType.MAPR_ONLY) {
      if ((admin = apacheHBaseAdmin_) == null) {
        synchronized (this) {
          admin = apacheHBaseAdmin_;
          if (admin == null) {
            try {
              admin = apacheHBaseAdmin_ = adminConstrcutor_.construct();
              return admin;
            }
            catch (Throwable t) {
              IOException ioe = null;
              Throwable ex = t;
              while (ex != null) {
                if (ex instanceof IOException) {
                  ioe = (IOException) ex;
                  break;
                }
                else if (ex instanceof InvocationTargetException) {
                  ex = ((InvocationTargetException)ex).getTargetException();
                }
                else {
                  ex = ex.getCause();
                }
              }
              admin = null;
              isHbaseAvailable_ = false;
              hbaseIOException_ = (ioe != null) ? ioe : new IOException(t);
              LOG.warn(ex.getMessage(), ex);
              if (throwException) {
                throw hbaseIOException_;
              }
            }
          }
        }
      }
    }
    return admin;
  }
}
