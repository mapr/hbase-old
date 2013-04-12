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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * <p>Used to communicate with a single HBase table.
 *
 * <p>This class is not thread safe for reads nor write.
 *
 * <p>In case of writes (Put, Delete), the underlying write buffer can
 * be corrupted if multiple threads contend over a single HTable instance.
 *
 * <p>In case of reads, some fields used by a Scan are shared among all threads.
 * The HTable implementation can either not contract to be safe in case of a Get
 *
 * <p>To access a table in a multi threaded environment, please consider
 * using the {@link HTablePool} class to create your HTable instances.
 *
 * <p>Instances of HTable passed the same {@link Configuration} instance will
 * share connections to servers out on the cluster and to the zookeeper ensemble
 * as well as caches of region locations.  This is usually a *good* thing and it
 * is recommended to reuse the same configuration object for all your tables.
 * This happens because they will all share the same underlying
 * {@link HConnection} instance. See {@link HConnectionManager} for more on
 * how this mechanism works.
 *
 * <p>{@link HConnection} will read most of the
 * configuration it needs from the passed {@link Configuration} on initial
 * construction.  Thereafter, for settings such as
 * <code>hbase.client.pause</code>, <code>hbase.client.retries.number</code>,
 * and <code>hbase.client.rpc.maxattempts</code> updating their values in the
 * passed {@link Configuration} subsequent to {@link HConnection} construction
 * will go unnoticed.  To run with changed values, make a new
 * {@link HTable} passing a new {@link Configuration} instance that has the
 * new configuration.
 *
 * <p>Note that this class implements the {@link Closeable} interface. When a
 * HTable instance is no longer required, it *should* be closed in order to ensure
 * that the underlying resources are promptly released. Please note that the close
 * method can throw java.io.IOException that must be handled.
 *
 * @see HBaseAdmin for create, drop, list, enable and disable of tables.
 * @see HConnection
 * @see HConnectionManager
 */
public class HTable implements HTableInterface, Closeable {
  private static final HTableInterfaceFactoryEx tableFactory = new HTableFactoryEx();
  private AbstractHTableInterface table = null;

  /**
   * Creates an object to access a HBase table.
   * Internally it creates a new instance of {@link Configuration} and a new
   * client to zookeeper as well as other resources.  It also comes up with
   * a fresh view of the cluster and must do discovery from scratch of region
   * locations; i.e. it will not make use of already-cached region locations if
   * available. Use only when being quick and dirty.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link #HTable(Configuration, String)}
   */
  @Deprecated
  public HTable(final String tableName)
  throws IOException {
    this(HBaseConfiguration.create(), Bytes.toBytes(tableName));
  }

  /**
   * Creates an object to access a HBase table.
   * Internally it creates a new instance of {@link Configuration} and a new
   * client to zookeeper as well as other resources.  It also comes up with
   * a fresh view of the cluster and must do discovery from scratch of region
   * locations; i.e. it will not make use of already-cached region locations if
   * available. Use only when being quick and dirty.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link #HTable(Configuration, String)}
   */
  @Deprecated
  public HTable(final byte [] tableName)
  throws IOException {
    this(HBaseConfiguration.create(), tableName);
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final String tableName)
  throws IOException {
    this(conf, Bytes.toBytes(tableName));
  }


  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final byte [] tableName)
  throws IOException {
    try {
      table = tableFactory.createHTableInterface(conf, tableName);
    } catch (Throwable e) {
      GenericHFactory.handleIOException(e);
    }
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>connection</code> instance.
   * Use this constructor when the ExecutorService and HConnection instance are
   * externally managed.
   * @param tableName Name of the table.
   * @param connection HConnection to be used.
   * @param pool ExecutorService to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(final byte[] tableName, final HConnection connection,
      final ExecutorService pool) throws IOException {
    try {
      table = tableFactory.createHTableInterface(tableName, connection, pool);
    } catch (Throwable e) {
      GenericHFactory.handleIOException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return table.getConfiguration();
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	* @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(HBaseConfiguration.create(), tableName);
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	* @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf, String tableName)
  throws IOException {
    return isTableEnabled(conf, Bytes.toBytes(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   */
  public static boolean isTableEnabled(Configuration conf,
      final byte[] tableName) throws IOException {
    Class<? extends AbstractHTableInterface> c =
        HTableFactoryEx.get().getImplementingClass(conf, tableName);
    try {
      Method isTableEnabled = c.getDeclaredMethod("isTableEnabled",
          new Class[] {Configuration.class, byte[].class });
      return (Boolean) isTableEnabled.invoke(null, conf, tableName);
    } catch (Exception e) {
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return The location of the given row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final String row)
  throws IOException {
    return table.getRegionLocation(Bytes.toBytes(row));
  }

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link #getRegionLocation(byte [], boolean)} instead
   */
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return table.getRegionLocation(row);
  }

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @param reload whether or not to reload information or just use cached
   * information
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final byte [] row, boolean reload)
  throws IOException {
    return table.getRegionLocation(row, reload);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte [] getTableName() {
    return table.getTableName();
  }

  /**
   * <em>INTERNAL</em> Used by unit tests and tools to do low-level
   * manipulations.
   * @return An HConnection instance.
   * @deprecated This method will be changed from public to package protected.
   */
  // TODO(tsuna): Remove this.  Unit tests shouldn't require public helpers.
  public HConnection getConnection() {
    return table.getConnection();
  }

  /**
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
   */
  public int getScannerCaching() {
    return table.getScannerCaching();
  }

  /**
   * Sets the number of rows that a scanner will fetch at once.
   * <p>
   * This will override the value specified by
   * {@code hbase.client.scanner.caching}.
   * Increasing this value will reduce the amount of work needed each time
   * {@code next()} is called on a scanner, at the expense of memory use
   * (since more rows will need to be maintained in memory by the scanners).
   * @param scannerCaching the number of rows a scanner will fetch at once.
   */
  public void setScannerCaching(int scannerCaching) {
    table.setScannerCaching(scannerCaching);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return table.getTableDescriptor();
  }

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte [][] getStartKeys() throws IOException {
    return table.getStartKeys();
  }

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte[][] getEndKeys() throws IOException {
    return table.getEndKeys();
  }

  /**
   * Gets the starting and ending row keys for every region in the currently
   * open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public Pair<byte[][],byte[][]> getStartEndKeys() throws IOException {
    return table.getStartEndKeys();
  }

  /**
   * Gets all the regions and their address for this table.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link #getRegionLocations()} or {@link #getStartEndKeys()}
   */
  public Map<HRegionInfo, HServerAddress> getRegionsInfo() throws IOException {
    return table.getRegionsInfo();
  }

  /**
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
    return table.getRegionLocations();
  }

  /**
   * Get the corresponding regions for an arbitrary range of keys.
   * <p>
   * @param startRow Starting row in range, inclusive
   * @param endRow Ending row in range, exclusive
   * @return A list of HRegionLocations corresponding to the regions that
   * contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  public List<HRegionLocation> getRegionsInRange(final byte [] startKey,
      final byte [] endKey) throws IOException {
    return table.getRegionsInRange(startKey, endKey);
  }

  /**
   * Save the passed region information and the table's regions
   * cache.
   * <p>
   * This is mainly useful for the MapReduce integration. You can call
   * {@link #deserializeRegionInfo deserializeRegionInfo}
   * to deserialize regions information from a
   * {@link DataInput}, then call this method to load them to cache.
   *
   * <pre>
   * {@code
   * HTable t1 = new HTable("foo");
   * FileInputStream fis = new FileInputStream("regions.dat");
   * DataInputStream dis = new DataInputStream(fis);
   *
   * Map<HRegionInfo, HServerAddress> hm = t1.deserializeRegionInfo(dis);
   * t1.prewarmRegionCache(hm);
   * }
   * </pre>
   * @param regionMap This piece of regions information will be loaded
   * to region cache.
   */
  public void prewarmRegionCache(Map<HRegionInfo, HServerAddress> regionMap) {
    table.prewarmRegionCache(regionMap);
  }

  /**
   * Serialize the regions information of this table and output
   * to <code>out</code>.
   * <p>
   * This is mainly useful for the MapReduce integration. A client could
   * perform a large scan for all the regions for the table, serialize the
   * region info to a file. MR job can ship a copy of the meta for the table in
   * the DistributedCache.
   * <pre>
   * {@code
   * FileOutputStream fos = new FileOutputStream("regions.dat");
   * DataOutputStream dos = new DataOutputStream(fos);
   * table.serializeRegionInfo(dos);
   * dos.flush();
   * dos.close();
   * }
   * </pre>
   * @param out {@link DataOutput} to serialize this object into.
   * @throws IOException if a remote or network exception occurs
   */
  public void serializeRegionInfo(DataOutput out) throws IOException {
    table.serializeRegionInfo(out);
  }

  /**
   * Read from <code>in</code> and deserialize the regions information.
   *
   * <p>It behaves similarly as {@link #getRegionsInfo getRegionsInfo}, except
   * that it loads the region map from a {@link DataInput} object.
   *
   * <p>It is supposed to be followed immediately by  {@link
   * #prewarmRegionCache prewarmRegionCache}.
   *
   * <p>
   * Please refer to {@link #prewarmRegionCache prewarmRegionCache} for usage.
   *
   * @param in {@link DataInput} object.
   * @return A map of HRegionInfo with its server address.
   * @throws IOException if an I/O exception occurs.
   */
  public Map<HRegionInfo, HServerAddress> deserializeRegionInfo(DataInput in)
  throws IOException {
    return table.deserializeRegionInfo(in);
  }

  /**
   * {@inheritDoc}
   */
   @Override
   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     return table.getRowOrBefore(row, family);
   }

   /**
    * {@inheritDoc}
    */
  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    return table.getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family) throws IOException {
    return table.getScanner(family);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    return table.getScanner(family, qualifier);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(final Get get) throws IOException {
    return table.get(get);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return table.get(gets);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void batch(final List<Row> actions, final Object[] results)
      throws InterruptedException, IOException {
    table.batch(actions, results);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized Object[] batch(final List<Row> actions) throws InterruptedException, IOException {
    return table.batch(actions);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final Delete delete)
  throws IOException {
    table.delete(delete);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    table.delete(deletes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final Put put) throws IOException {
    table.put(put);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final List<Put> puts) throws IOException {
    table.put(puts);
  }

  /**
   * {@inheritDoc}
   */
  public void mutateRow(final RowMutations rm) throws IOException {
    table.mutateRow(rm);
  }

  /**
   * {@inheritDoc}
   */
  public Result append(final Append append) throws IOException {
    return table.append(append);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(final Increment increment) throws IOException {
    return table.increment(increment);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL)
  throws IOException {
    return table.incrementColumnValue(row, family, qualifier, amount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    return table.checkAndPut(row, family, qualifier, value, put);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    return table.checkAndDelete(row, family, qualifier, value, delete);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(final Get get) throws IOException {
    return table.exists(get);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws IOException {
    table.flushCommits();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    table.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowLock lockRow(final byte [] row)
  throws IOException {
    return table.lockRow(row);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void unlockRow(final RowLock rl)
  throws IOException {
    table.unlockRow(rl);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    return table.isAutoFlush();
  }

  /**
   * See {@link #setAutoFlush(boolean, boolean)}
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   */
  public void setAutoFlush(boolean autoFlush) {
    setAutoFlush(autoFlush, autoFlush);
  }

  /**
   * Turns 'auto-flush' on or off.
   * <p>
   * When enabled (default), {@link Put} operations don't get buffered/delayed
   * and are immediately executed. Failed operations are not retried. This is
   * slower but safer.
   * <p>
   * Turning off {@link #autoFlush} means that multiple {@link Put}s will be
   * accepted before any RPC is actually sent to do the write operations. If the
   * application dies before pending writes get flushed to HBase, data will be
   * lost.
   * <p>
   * When you turn {@link #autoFlush} off, you should also consider the
   * {@link #clearBufferOnFail} option. By default, asynchronous {@link Put}
   * requests will be retried on failure until successful. However, this can
   * pollute the writeBuffer and slow down batching performance. Additionally,
   * you may want to issue a number of Put requests and call
   * {@link #flushCommits()} as a barrier. In both use cases, consider setting
   * clearBufferOnFail to true to erase the buffer after {@link #flushCommits()}
   * has been called, regardless of success.
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   * @param clearBufferOnFail
   *          Whether to keep Put failures in the writeBuffer
   * @see #flushCommits
   */
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    table.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter
   * {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  public long getWriteBufferSize() {
    return table.getWriteBufferSize();
  }

  /**
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the
   * write buffer, the buffer gets flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   */
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    table.setWriteBufferSize(writeBufferSize);
  }

  /**
   * Returns the write buffer.
   * @return The current write buffer.
   */
  public ArrayList<Put> getWriteBuffer() {
    return table.getWriteBuffer();
  }

  /**
   * The pool is used for mutli requests for this HTable
   * @return the pool used for mutli
   */
  ExecutorService getPool() {
    return table.getPool();
  }

  static class DaemonThreadFactory implements ThreadFactory {
    static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        DaemonThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null)? s.getThreadGroup() :
                                 Thread.currentThread().getThreadGroup();
            namePrefix = "htable-pool-" +
                          poolNumber.getAndIncrement() +
                         "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                                  namePrefix + threadNumber.getAndIncrement(),
                                  0);
            if (!t.isDaemon()) {
              t.setDaemon(true);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
              t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   * @throws IOException
   */
  public static void setRegionCachePrefetch(final byte[] tableName,
      final boolean enable) throws IOException {
    Class<? extends AbstractHTableInterface> c =
        HTableFactoryEx.get().getImplementingClass(null, tableName);
    try {
      Method setRegionCachePrefetch = c.getDeclaredMethod("setRegionCachePrefetch",
          new Class[] {byte[].class, boolean.class});
      setRegionCachePrefetch.invoke(null, tableName, enable);
    } catch (Exception e) {
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param conf The Configuration object to use.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   * @throws IOException
   */
  public static void setRegionCachePrefetch(final Configuration conf,
      final byte[] tableName, final boolean enable) throws IOException {
    Class<? extends AbstractHTableInterface> c =
        HTableFactoryEx.get().getImplementingClass(conf, tableName);
    try {
      Method setRegionCachePrefetch = c.getDeclaredMethod("setRegionCachePrefetch",
          new Class[] {Configuration.class, byte[].class, boolean.class});
      setRegionCachePrefetch.invoke(null, conf, tableName, enable);
    } catch (Exception e) {
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param conf The Configuration object to use.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final Configuration conf,
      final byte[] tableName) throws IOException {
    Class<? extends AbstractHTableInterface> c =
        HTableFactoryEx.get().getImplementingClass(conf, tableName);
    try {
      Method getRegionCachePrefetch = c.getDeclaredMethod("getRegionCachePrefetch",
          new Class[] {Configuration.class, byte[].class});
      return (Boolean) getRegionCachePrefetch.invoke(null, conf, tableName);
    } catch (Exception e) {
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final byte[] tableName)
      throws IOException {
    Class<? extends AbstractHTableInterface> c =
        HTableFactoryEx.get().getImplementingClass(null, tableName);
    try {
      Method getRegionCachePrefetch = c.getDeclaredMethod("getRegionCachePrefetch",
          new Class[] {byte[].class});
      return (Boolean) getRegionCachePrefetch.invoke(null, tableName);
    } catch (Exception e) {
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
  }

  /**
   * Explicitly clears the region cache to fetch the latest value from META.
   * This is a power user function: avoid unless you know the ramifications.
   */
  public void clearRegionCache() {
    table.clearRegionCache();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(
      Class<T> protocol, byte[] row) {
    return table.coprocessorProxy(protocol, row);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[],R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable)
      throws IOException, Throwable {
    return table.coprocessorExec(protocol, startKey, endKey, callable);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable, Batch.Callback<R> callback)
      throws IOException, Throwable {
    table.coprocessorExec(protocol, startKey, endKey,
                             callable, callback);
  }

  public void setOperationTimeout(int operationTimeout) {
    table.setOperationTimeout(operationTimeout);
  }

  public int getOperationTimeout() {
    return table.getOperationTimeout();
  }

}
