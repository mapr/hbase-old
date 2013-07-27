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
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.mapr.AbstractHTable;
import org.apache.hadoop.hbase.client.mapr.BaseTableMappingRules;
import org.apache.hadoop.hbase.client.mapr.GenericHFactory;
import org.apache.hadoop.hbase.client.mapr.TableMappingRulesFactory;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.ExecRPCInvoker;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
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
  private static final GenericHFactory<AbstractHTable> tableFactory_ =
      new GenericHFactory<AbstractHTable>();
  private BaseTableMappingRules tableMappingRule_;
  private final AbstractHTable maprTable_;

  private static final Log LOG = LogFactory.getLog(HTable.class);
  private HConnection connection;
  private byte [] tableName;
  protected int scannerTimeout;
  private volatile Configuration configuration;
  private ArrayList<Put> writeBuffer = new ArrayList<Put>();
  private long writeBufferSize;
  private boolean clearBufferOnFail;
  private boolean autoFlush;
  private long currentWriteBufferSize;
  protected int scannerCaching;
  private int maxKeyValueSize;
  private ExecutorService pool;  // For Multi
  private long maxScannerResultSize;
  private boolean closed;
  private int operationTimeout;
  private static final int DOPUT_WB_CHECK = 10;    // i.e., doPut checks the writebuffer every X Puts.
  private boolean cleanupOnClose; // close the connection in close()

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
    if ((maprTable_ = createdMapRTable(conf, tableName)) != null) {
      return; // If it was a MapR table, our work is done
    }

    this.tableName = FSUtils.adjustTableName(tableName);
    this.cleanupOnClose = true;
    if (conf == null) {
      this.scannerTimeout = 0;
      this.connection = null;
      return;
    }
    this.connection = HConnectionManager.getConnection(conf);
    this.configuration = conf;

    int maxThreads = conf.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
    if (maxThreads == 0) {
      maxThreads = 1; // is there a better default?
    }
    long keepAliveTime = conf.getLong("hbase.htable.threads.keepalivetime", 60);

    // Using the "direct handoff" approach, new threads will only be created
    // if it is necessary and will grow unbounded. This could be bad but in HCM
    // we only create as many Runnables as there are region servers. It means
    // it also scales when new region servers are added.
    this.pool = new ThreadPoolExecutor(1, maxThreads,
        keepAliveTime, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new DaemonThreadFactory());
    ((ThreadPoolExecutor)this.pool).allowCoreThreadTimeOut(true);

    this.finishSetup();
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
    if ((maprTable_ = createdMapRTable(connection.getConfiguration(), tableName)) != null) {
      // If it was a MapR table, our work is done
      return;
    }

    if (pool == null || pool.isShutdown()) {
      throw new IllegalArgumentException("Pool is null or shut down.");
    }
    if (connection == null || connection.isClosed()) {
      throw new IllegalArgumentException("Connection is null or closed.");
    }
    this.tableName = FSUtils.adjustTableName(tableName);
    this.cleanupOnClose = false;
    this.connection = connection;
    this.configuration = connection.getConfiguration();
    this.pool = pool;

    this.finishSetup();
  }

  /**
   * setup this HTable's parameter based on the passed configuration
   * @param conf
   */
  private void finishSetup() throws IOException {
    this.connection.locateRegion(tableName, HConstants.EMPTY_START_ROW);
    this.scannerTimeout = (int) this.configuration.getLong(
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
        HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD);
    this.operationTimeout = HTableDescriptor.isMetaTable(tableName) ? HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT
        : this.configuration.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
            HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.writeBufferSize = this.configuration.getLong(
        "hbase.client.write.buffer", 2097152);
    this.clearBufferOnFail = true;
    this.autoFlush = true;
    this.currentWriteBufferSize = 0;
    this.scannerCaching = this.configuration.getInt(
        "hbase.client.scanner.caching", 1);

    this.maxScannerResultSize = this.configuration.getLong(
        HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    this.maxKeyValueSize = this.configuration.getInt(
        "hbase.client.keyvalue.maxsize", -1);
    this.closed = false;
  }

  /**
   * Tests if the table identified by tableName should be considered
   * as a MapR table according to table mapping rules and if yes 
   * create a MapR table instance
   *
   * @param conf
   * @param tableName
   * @return true if this is a MapR table
   * @throws IOException
   */
  private AbstractHTable createdMapRTable(Configuration conf,
      byte[] tableName) throws IOException {
    tableMappingRule_ = TableMappingRulesFactory.create(conf);
    if (tableMappingRule_.isMapRTable(tableName)) {
      try {
        configuration = conf;
        return tableFactory_.getImplementorInstance(
          configuration,
          configuration.get("htable.impl.mapr", "com.mapr.fs.HTableImpl"),
          new Object[] {conf, tableName},
          new Class[] {Configuration.class, byte[].class});
      } catch (Throwable e) {
        GenericHFactory.handleIOException(e);
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * <b>Return <code>true</code> for MapR Tables.</b><p>
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
   * <b>Return <code>true</code> for MapR Tables.</b><p>
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
   * <b>Return <code>true</code> for MapR Tables.</b><p>
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
   * <b>Return <code>true</code> for MapR Tables.</b><p>
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   */
  public static boolean isTableEnabled(Configuration conf,
      final byte[] tableName) throws IOException {
    if (TableMappingRulesFactory.create(conf).isMapRTable(tableName)) {
      return true;
    }
    return HConnectionManager.execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.isTableEnabled(FSUtils.adjustTableName(tableName));
      }
    });
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return The location of the given row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final String row)
  throws IOException {
    if (maprTable_ != null) {
      return maprTable_.getRegionLocation(row);
    }
    return connection.getRegionLocation(tableName, Bytes.toBytes(row), false);
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
    if (maprTable_ != null) {
      return maprTable_.getRegionLocation(row);
    }
    return connection.getRegionLocation(tableName, row, false);
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
    if (maprTable_ != null) {
      return maprTable_.getRegionLocation(row, reload);
    }
    return connection.getRegionLocation(tableName, row, reload);
  }
     
  /**
   * {@inheritDoc}
   */
  @Override
  public byte [] getTableName() {
    if (maprTable_ != null) {
      return maprTable_.getTableName();
    }
    return this.tableName;
  }

  /**
   * <b>Return <code>null</code> for MapR Tables.</b><p>
   * <em>INTERNAL</em> Used by unit tests and tools to do low-level
   * manipulations.
   * @return An HConnection instance.
   * @deprecated This method will be changed from public to package protected.
   */
  // TODO(tsuna): Remove this.  Unit tests shouldn't require public helpers.
  public HConnection getConnection() {
    return this.connection;
  }

  /**
   * <b>NO-OP for MapR Tables.</b><p>
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
   */
  public int getScannerCaching() {
    return scannerCaching;
  }

  /**
   * <b>NO-OP for MapR Tables.</b><p>
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
    this.scannerCaching = scannerCaching;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    if (maprTable_ != null) {
      return maprTable_.getTableDescriptor();
    }
    return new UnmodifyableHTableDescriptor(
      this.connection.getHTableDescriptor(this.tableName));
  }

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte [][] getStartKeys() throws IOException {
    if (maprTable_ != null) {
      return maprTable_.getStartKeys();
    }
    return getStartEndKeys().getFirst();
  }

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte[][] getEndKeys() throws IOException {
    if (maprTable_ != null) {
      return maprTable_.getEndKeys();
    }
    return getStartEndKeys().getSecond();
  }

  /**
   * Gets the starting and ending row keys for every region in the currently
   * open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  @SuppressWarnings("unchecked")
  public Pair<byte[][],byte[][]> getStartEndKeys() throws IOException {
    if (maprTable_ != null) {
      return maprTable_.getStartEndKeys();
    }
    NavigableMap<HRegionInfo, ServerName> regions = getRegionLocations();
    final List<byte[]> startKeyList = new ArrayList<byte[]>(regions.size());
    final List<byte[]> endKeyList = new ArrayList<byte[]>(regions.size());

    for (HRegionInfo region : regions.keySet()) {
      startKeyList.add(region.getStartKey());
      endKeyList.add(region.getEndKey());
    }

    return new Pair<byte [][], byte [][]>(
      startKeyList.toArray(new byte[startKeyList.size()][]),
      endKeyList.toArray(new byte[endKeyList.size()][]));
  }

  /**
   * Gets all the regions and their address for this table.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link #getRegionLocations()} or {@link #getStartEndKeys()}
   */
  public Map<HRegionInfo, HServerAddress> getRegionsInfo() throws IOException {
    final Map<HRegionInfo, HServerAddress> regionMap =
      new TreeMap<HRegionInfo, HServerAddress>();

    final Map<HRegionInfo, ServerName> regionLocations = getRegionLocations();

    for (Map.Entry<HRegionInfo, ServerName> entry : regionLocations.entrySet()) {
      HServerAddress server = new HServerAddress();
      ServerName serverName = entry.getValue();
      if (serverName != null && serverName.getHostAndPort() != null) {
        server = new HServerAddress(Addressing.createInetSocketAddressFromHostAndPortStr(
            serverName.getHostAndPort()));
      }
      regionMap.put(entry.getKey(), server);
    }

    return regionMap;
  }

  /**
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
    if (maprTable_ != null) {
      return maprTable_.getRegionLocations();
    }
    return MetaScanner.allTableRegions(getConfiguration(), getTableName(), false);
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
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey,
                                                    HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException(
        "Invalid range: " + Bytes.toStringBinary(startKey) +
        " > " + Bytes.toStringBinary(endKey));
    }
    final List<HRegionLocation> regionList = new ArrayList<HRegionLocation>();
    byte [] currentKey = startKey;
    do {
      HRegionLocation regionLocation = getRegionLocation(currentKey, false);
      regionList.add(regionLocation);
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW) &&
             (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
    return regionList;
  }

  /**
   * <b>NO-OP for MapR Tables.</b><p>
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
    if (maprTable_ != null) {
      return;
    }
    this.connection.prewarmRegionCache(this.getTableName(), regionMap);
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
    Map<HRegionInfo, HServerAddress> allRegions = this.getRegionsInfo();
    // first, write number of regions
    out.writeInt(allRegions.size());
    for (Map.Entry<HRegionInfo, HServerAddress> es : allRegions.entrySet()) {
      es.getKey().write(out);
      es.getValue().write(out);
    }
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
    final Map<HRegionInfo, HServerAddress> allRegions =
      new TreeMap<HRegionInfo, HServerAddress>();

    // the first integer is expected to be the size of records
    int regionsCount = in.readInt();
    for (int i = 0; i < regionsCount; ++i) {
      HRegionInfo hri = new HRegionInfo();
      hri.readFields(in);
      HServerAddress hsa = new HServerAddress();
      hsa.readFields(in);
      allRegions.put(hri, hsa);
    }
    return allRegions;
  }

  /**
   * {@inheritDoc}
   */
   @Override
   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     if (maprTable_ != null) {
       return maprTable_.getRowOrBefore(row, family);
     }
     return connection.getRegionServerWithRetries(
         new ServerCallable<Result>(connection, tableName, row, operationTimeout) {
       public Result call() throws IOException {
         return server.getClosestRowBefore(location.getRegionInfo().getRegionName(),
           row, family);
       }
     });
   }

   /**
    * {@inheritDoc}
    */
  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    if (maprTable_ != null) {
      return maprTable_.getScanner(scan);
    }
    ClientScanner s = new ClientScanner(scan);
    s.initialize();
    return s;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(final Get get) throws IOException {
    if (maprTable_ != null) {
      return maprTable_.get(get);
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Result>(connection, tableName, get.getRow(), operationTimeout) {
          public Result call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (maprTable_ != null) {
      return maprTable_.get(gets);
    }
    try {
      Object [] r1 = batch((List)gets);

      // translate.
      Result [] results = new Result[r1.length];
      int i=0;
      for (Object o : r1) {
        // batch ensures if there is a failure we get an exception instead
        results[i++] = (Result) o;
      }

      return results;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void batch(final List<Row> actions, final Object[] results)
      throws InterruptedException, IOException {
    if (maprTable_ != null) {
      maprTable_.batch(actions, results);
      return;
    }
    connection.processBatch(actions, tableName, pool, results);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized Object[] batch(final List<Row> actions) throws InterruptedException, IOException {
    if (maprTable_ != null) {
      return maprTable_.batch(actions);
    }
    Object[] results = new Object[actions.size()];
    connection.processBatch(actions, tableName, pool, results);
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final Delete delete)
  throws IOException {
    if (maprTable_ != null) {
      maprTable_.delete(delete);
      return;
    }
    connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, delete.getRow(), operationTimeout) {
          public Boolean call() throws IOException {
            server.delete(location.getRegionInfo().getRegionName(), delete);
            return null; // FindBugs NP_BOOLEAN_RETURN_NULL
          }
        }
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    if (maprTable_ != null) {
      maprTable_.delete(deletes);
      return;
    }
    Object[] results = new Object[deletes.size()];
    try {
      connection.processBatch((List) deletes, tableName, pool, results);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      // mutate list so that it is empty for complete success, or contains only failed records
      // results are returned in the same order as the requests in list
      // walk the list backwards, so we can remove from list without impacting the indexes of earlier members
      for (int i = results.length - 1; i>=0; i--) {
        // if result is not null, it succeeded
        if (results[i] instanceof Result) {
          deletes.remove(i);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final Put put) throws IOException {
    if (maprTable_ != null) {
      maprTable_.put(put);
      return;
    }
    doPut(Arrays.asList(put));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final List<Put> puts) throws IOException {
    if (maprTable_ != null) {
      maprTable_.put(puts);
      return;
    }
    doPut(puts);
  }

  private void doPut(final List<Put> puts) throws IOException {
    int n = 0;
    for (Put put : puts) {
      validatePut(put);
      writeBuffer.add(put);
      currentWriteBufferSize += put.heapSize();
     
      // we need to periodically see if the writebuffer is full instead of waiting until the end of the List
      n++;
      if (n % DOPUT_WB_CHECK == 0 && currentWriteBufferSize > writeBufferSize) {
        flushCommits();
      }
    }
    if (autoFlush || currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  public Result append(Append a) throws IOException {
    if (maprTable_ != null) {
      return maprTable_.append(a);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(final Increment increment) throws IOException {
    if (!increment.hasFamilies()) {
      throw new IOException(
          "Invalid arguments to increment, no columns specified");
    }
    if (maprTable_ != null) {
      return maprTable_.increment(increment);
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Result>(connection, tableName, increment.getRow(), operationTimeout) {
          public Result call() throws IOException {
            return server.increment(
                location.getRegionInfo().getRegionName(), increment);
          }
        }
    );
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
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("column is null");
    }
    if (npe != null) {
      throw new IOException(
          "Invalid arguments to incrementColumnValue", npe);
    }
    if (maprTable_ != null) {
      return maprTable_.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Long>(connection, tableName, row, operationTimeout) {
          public Long call() throws IOException {
            return server.incrementColumnValue(
                location.getRegionInfo().getRegionName(), row, family,
                qualifier, amount, writeToWAL);
          }
        }
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    if (maprTable_ != null) {
      return maprTable_.checkAndPut(row, family, qualifier, value, put);
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row, operationTimeout) {
          public Boolean call() throws IOException {
            return server.checkAndPut(location.getRegionInfo().getRegionName(),
                row, family, qualifier, value, put) ? Boolean.TRUE : Boolean.FALSE;
          }
        }
    );
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    if (maprTable_ != null) {
      return maprTable_.checkAndDelete(row, family, qualifier, value, delete);
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row, operationTimeout) {
          public Boolean call() throws IOException {
            return server.checkAndDelete(
                location.getRegionInfo().getRegionName(),
                row, family, qualifier, value, delete)
            ? Boolean.TRUE : Boolean.FALSE;
          }
        }
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(final Get get) throws IOException {
    if (maprTable_ != null) {
      return maprTable_.exists(get);
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, get.getRow(), operationTimeout) {
          public Boolean call() throws IOException {
            return server.
                exists(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws IOException {
    if (maprTable_ != null) {
      maprTable_.flushCommits();
      return;
    }
    try {
      Object[] results = new Object[writeBuffer.size()];
      try {
        this.connection.processBatch(writeBuffer, tableName, pool, results);
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        // mutate list so that it is empty for complete success, or contains
        // only failed records results are returned in the same order as the
        // requests in list walk the list backwards, so we can remove from list
        // without impacting the indexes of earlier members
        for (int i = results.length - 1; i>=0; i--) {
          if (results[i] instanceof Result) {
            // successful Puts are removed from the list here.
            writeBuffer.remove(i);
          }
        }
      }
    } finally {
      if (clearBufferOnFail) {
        writeBuffer.clear();
        currentWriteBufferSize = 0;
      } else {
        // the write buffer was adjusted by processBatchOfPuts
        currentWriteBufferSize = 0;
        for (Put aPut : writeBuffer) {
          currentWriteBufferSize += aPut.heapSize();
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    if (maprTable_ != null) {
      maprTable_.close();
      return;
    }
    if (this.closed) {
      return;
    }
    flushCommits();
    if (cleanupOnClose) {
      this.pool.shutdown();
      if (this.connection != null) {
        this.connection.close();
      }
    }
    this.closed = true;
  }

  // validate for well-formedness
  private void validatePut(final Put put) throws IllegalArgumentException{
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<KeyValue> list : put.getFamilyMap().values()) {
        for (KeyValue kv : list) {
          if (kv.getLength() > maxKeyValueSize) {
            throw new IllegalArgumentException("KeyValue size too large");
          }
        }
      }
    }
  }

  /**
   * <b>NO-OP for MapR Tables.</b><p>
   * {@inheritDoc}
   */
  @Override
  public RowLock lockRow(final byte [] row)
  throws IOException {
    if (maprTable_ != null) {
      return maprTable_.lockRow(row);
    }
    return connection.getRegionServerWithRetries(
      new ServerCallable<RowLock>(connection, tableName, row, operationTimeout) {
        public RowLock call() throws IOException {
          long lockId =
              server.lockRow(location.getRegionInfo().getRegionName(), row);
          return new RowLock(row,lockId);
        }
      }
    );
  }

  /**
   * <b>NO-OP for MapR Tables.</b><p>
   * {@inheritDoc}
   */
  @Override
  public void unlockRow(final RowLock rl)
  throws IOException {
    if (maprTable_ != null) {
      maprTable_.unlockRow(rl);
      return;
    }
    connection.getRegionServerWithRetries(
      new ServerCallable<Boolean>(connection, tableName, rl.getRow(), operationTimeout) {
        public Boolean call() throws IOException {
          server.unlockRow(location.getRegionInfo().getRegionName(),
              rl.getLockId());
          return null; // FindBugs NP_BOOLEAN_RETURN_NULL
        }
      }
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    if (maprTable_ != null) {
      return maprTable_.isAutoFlush();
    }
    return autoFlush;
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
    if (maprTable_ != null) {
      maprTable_.setAutoFlush(autoFlush, clearBufferOnFail);
      return;
    }
    this.autoFlush = autoFlush;
    this.clearBufferOnFail = autoFlush || clearBufferOnFail;
  }

  /**
   * <b>Return <code>0</code> for MapR Tables.</b><p>
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter
   * {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  public long getWriteBufferSize() {
    if (maprTable_ != null) {
      LOG.warn("getWriteBufferSize() called for a MapR Table, returning 0.");
      return 0;
    }
    return writeBufferSize;
  }

  /**
   * <b>NO-OP for MapR Tables.</b><p>
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the
   * write buffer, the buffer gets flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   */
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    if (maprTable_ != null) {
      LOG.warn("setWriteBufferSize() called for a MapR Table, silently ignoring.");
      return;
    }
    this.writeBufferSize = writeBufferSize;
    if(currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * <b>Return <code>null</code> for MapR Tables.</b><p>
   * Returns the write buffer.
   * @return The current write buffer.
   */
  public ArrayList<Put> getWriteBuffer() {
    if (maprTable_ != null) {
      return null;
    }
    return writeBuffer;
  }

  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  protected class ClientScanner implements ResultScanner {
    private final Log CLIENT_LOG = LogFactory.getLog(this.getClass());
    // HEADSUP: The scan internal start row can change as we move through table.
    private Scan scan;
    private boolean closed = false;
    // Current region scanner is against.  Gets cleared if current region goes
    // wonky: e.g. if it splits on us.
    private HRegionInfo currentRegion = null;
    private ScannerCallable callable = null;
    private final LinkedList<Result> cache = new LinkedList<Result>();
    private final int caching;
    private long lastNext;
    // Keep lastResult returned successfully in case we have to reset scanner.
    private Result lastResult = null;

    protected ClientScanner(final Scan scan) {
      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Creating scanner over "
            + Bytes.toString(getTableName())
            + " starting at key '" + Bytes.toStringBinary(scan.getStartRow()) + "'");
      }
      this.scan = scan;
      this.lastNext = System.currentTimeMillis();

      // Use the caching from the Scan.  If not set, use the default cache setting for this table.
      if (this.scan.getCaching() > 0) {
        this.caching = this.scan.getCaching();
      } else {
        this.caching = HTable.this.scannerCaching;
      }

      // Removed filter validation.  We have a new format now, only one of all
      // the current filters has a validate() method.  We can add it back,
      // need to decide on what we're going to do re: filter redesign.
      // Need, at the least, to break up family from qualifier as separate
      // checks, I think it's important server-side filters are optimal in that
      // respect.
    }

    public void initialize() throws IOException {
      nextScanner(this.caching, false);
    }

    protected Scan getScan() {
      return scan;
    }

    protected long getTimestamp() {
      return lastNext;
    }

    // returns true if the passed region endKey
    private boolean checkScanStopRow(final byte [] endKey) {
      if (this.scan.getStopRow().length > 0) {
        // there is a stop row, check to see if we are past it.
        byte [] stopRow = scan.getStopRow();
        int cmp = Bytes.compareTo(stopRow, 0, stopRow.length,
          endKey, 0, endKey.length);
        if (cmp <= 0) {
          // stopRow <= endKey (endKey is equals to or larger than stopRow)
          // This is a stop.
          return true;
        }
      }
      return false; //unlikely.
    }

    /*
     * Gets a scanner for the next region.  If this.currentRegion != null, then
     * we will move to the endrow of this.currentRegion.  Else we will get
     * scanner at the scan.getStartRow().  We will go no further, just tidy
     * up outstanding scanners, if <code>currentRegion != null</code> and
     * <code>done</code> is true.
     * @param nbRows
     * @param done Server-side says we're done scanning.
     */
    private boolean nextScanner(int nbRows, final boolean done)
    throws IOException {
      // Close the previous scanner if it's open
      if (this.callable != null) {
        this.callable.setClose();
        getConnection().getRegionServerWithRetries(callable);
        this.callable = null;
      }

      // Where to start the next scanner
      byte [] localStartKey;

      // if we're at end of table, close and return false to stop iterating
      if (this.currentRegion != null) {
        byte [] endKey = this.currentRegion.getEndKey();
        if (endKey == null ||
            Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
            checkScanStopRow(endKey) ||
            done) {
          close();
          if (CLIENT_LOG.isDebugEnabled()) {
            CLIENT_LOG.debug("Finished with scanning at " + this.currentRegion);
          }
          return false;
        }
        localStartKey = endKey;
        if (CLIENT_LOG.isDebugEnabled()) {
          CLIENT_LOG.debug("Finished with region " + this.currentRegion);
        }
      } else {
        localStartKey = this.scan.getStartRow();
      }

      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Advancing internal scanner to startKey at '" +
          Bytes.toStringBinary(localStartKey) + "'");
      }
      try {
        callable = getScannerCallable(localStartKey, nbRows);
        // Open a scanner on the region server starting at the
        // beginning of the region
        getConnection().getRegionServerWithRetries(callable);
        this.currentRegion = callable.getHRegionInfo();
      } catch (IOException e) {
        close();
        throw e;
      }
      return true;
    }

    protected ScannerCallable getScannerCallable(byte [] localStartKey,
        int nbRows) {
      scan.setStartRow(localStartKey);
      ScannerCallable s = new ScannerCallable(getConnection(),
        getTableName(), scan);
      s.setCaching(nbRows);
      return s;
    }

    public Result next() throws IOException {
      // If the scanner is closed but there is some rows left in the cache,
      // it will first empty it before returning null
      if (cache.size() == 0 && this.closed) {
        return null;
      }
      if (cache.size() == 0) {
        Result [] values = null;
        long remainingResultSize = maxScannerResultSize;
        int countdown = this.caching;
        // We need to reset it if it's a new callable that was created
        // with a countdown in nextScanner
        callable.setCaching(this.caching);
        // This flag is set when we want to skip the result returned.  We do
        // this when we reset scanner because it split under us.
        boolean skipFirst = false;
        do {
          try {
            if (skipFirst) {
              // Skip only the first row (which was the last row of the last
              // already-processed batch).
              callable.setCaching(1);
              values = getConnection().getRegionServerWithRetries(callable);
              callable.setCaching(this.caching);
              skipFirst = false;
            }
            // Server returns a null values if scanning is to stop.  Else,
            // returns an empty array if scanning is to go on and we've just
            // exhausted current region.
            values = getConnection().getRegionServerWithRetries(callable);
          } catch (DoNotRetryIOException e) {
            if (e instanceof UnknownScannerException) {
              long timeout = lastNext + scannerTimeout;
              // If we are over the timeout, throw this exception to the client
              // Else, it's because the region moved and we used the old id
              // against the new region server; reset the scanner.
              if (timeout < System.currentTimeMillis()) {
                long elapsed = System.currentTimeMillis() - lastNext;
                ScannerTimeoutException ex = new ScannerTimeoutException(
                    elapsed + "ms passed since the last invocation, " +
                        "timeout is currently set to " + scannerTimeout);
                ex.initCause(e);
                throw ex;
              }
            } else {
              Throwable cause = e.getCause();
              if (cause == null || (!(cause instanceof NotServingRegionException)
                  && !(cause instanceof RegionServerStoppedException))) {
                throw e;
              }
            }
            // Else, its signal from depths of ScannerCallable that we got an
            // NSRE on a next and that we need to reset the scanner.
            if (this.lastResult != null) {
              this.scan.setStartRow(this.lastResult.getRow());
              // Skip first row returned.  We already let it out on previous
              // invocation.
              skipFirst = true;
            }
            // Clear region
            this.currentRegion = null;
            continue;
          }
          lastNext = System.currentTimeMillis();
          if (values != null && values.length > 0) {
            for (Result rs : values) {
              cache.add(rs);
              for (KeyValue kv : rs.raw()) {
                  remainingResultSize -= kv.heapSize();
              }
              countdown--;
              this.lastResult = rs;
            }
          }
          // Values == null means server-side filter has determined we must STOP
        } while (remainingResultSize > 0 && countdown > 0 && nextScanner(countdown, values == null));
      }

      if (cache.size() > 0) {
        return cache.poll();
      }
      return null;
    }

    /**
     * Get <param>nbRows</param> rows.
     * How many RPCs are made is determined by the {@link Scan#setCaching(int)}
     * setting (or hbase.client.scanner.caching in hbase-site.xml).
     * @param nbRows number of rows to return
     * @return Between zero and <param>nbRows</param> RowResults.  Scan is done
     * if returned array is of zero-length (We never return null).
     * @throws IOException
     */
    public Result [] next(int nbRows) throws IOException {
      // Collect values to be returned here
      ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
      for(int i = 0; i < nbRows; i++) {
        Result next = next();
        if (next != null) {
          resultSets.add(next);
        } else {
          break;
        }
      }
      return resultSets.toArray(new Result[resultSets.size()]);
    }

    public void close() {
      if (callable != null) {
        callable.setClose();
        try {
          getConnection().getRegionServerWithRetries(callable);
        } catch (IOException e) {
          // We used to catch this error, interpret, and rethrow. However, we
          // have since decided that it's not nice for a scanner's close to
          // throw exceptions. Chances are it was just an UnknownScanner
          // exception due to lease time out.
        }
        callable = null;
      }
      closed = true;
    }

    public Iterator<Result> iterator() {
      return new Iterator<Result>() {
        // The next RowResult, possibly pre-read
        Result next = null;

        // return true if there is another item pending, false if there isn't.
        // this method is where the actual advancing takes place, but you need
        // to call next() to consume it. hasNext() will only advance if there
        // isn't a pending next().
        public boolean hasNext() {
          if (next == null) {
            try {
              next = ClientScanner.this.next();
              return next != null;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          return true;
        }

        // get the pending next item and advance the iterator. returns null if
        // there is no next item.
        public Result next() {
          // since hasNext() does the real advancing, we call this to determine
          // if there is a next before proceeding.
          if (!hasNext()) {
            return null;
          }

          // if we get to here, then hasNext() has given us an item to return.
          // we want to return the item and then null out the next pointer, so
          // we use a temporary variable.
          Result temp = next;
          next = null;
          return temp;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  /**
   * The pool is used for mutli requests for this HTable
   * @return the pool used for mutli
   */
  ExecutorService getPool() {
    if (maprTable_ != null) {
      return null;
    }
    return this.pool;
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
   * <b>NO-OP for MapR Tables.</b><p>
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
    if (TableMappingRulesFactory.create(
      HBaseConfiguration.create()).isMapRTable(tableName)) {
      return;
    }
    HConnectionManager.execute(new HConnectable<Void>(HBaseConfiguration
        .create()) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        connection.setRegionCachePrefetch(tableName, enable);
        return null;
      }
    });
  }

  /**
   * <b>NO-OP for MapR Tables.</b><p>
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
    if (TableMappingRulesFactory.create(
      HBaseConfiguration.create()).isMapRTable(tableName)) {
      return;
    }
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        connection.setRegionCachePrefetch(tableName, enable);
        return null;
      }
    });
  }

  /**
   * <b>Return <code>false</code> for MapR Tables.</b><p>
   * Check whether region cache prefetch is enabled or not for the table.
   * @param conf The Configuration object to use.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final Configuration conf,
      final byte[] tableName) throws IOException {
    if (TableMappingRulesFactory.create(
      HBaseConfiguration.create()).isMapRTable(tableName)) {
      return false;
    }
    return HConnectionManager.execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.getRegionCachePrefetch(tableName);
      }
    });
  }

  /**
   * <b>Return <code>false</code> for MapR Tables.</b><p>
   * Check whether region cache prefetch is enabled or not for the table.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final byte[] tableName) throws IOException {
    if (TableMappingRulesFactory.create(
      HBaseConfiguration.create()).isMapRTable(tableName)) {
      return false;
    }
    return HConnectionManager.execute(new HConnectable<Boolean>(
        HBaseConfiguration.create()) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.getRegionCachePrefetch(tableName);
      }
    });
 }

  /**
   * <b>NO-OP for MapR Tables</b><p>
   * Explicitly clears the region cache to fetch the latest value from META.
   * This is a power user function: avoid unless you know the ramifications.
   */
  public void clearRegionCache() {
    if (maprTable_ != null) {
      maprTable_.clearRegionCache();
      return;
    }
    this.connection.clearRegionCache();
  }

  /**
   * <b>NO-OP for MapR Tables, returns <code>null</code>.</b><p>
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(
      Class<T> protocol, byte[] row) {
    if (maprTable_ != null) {
      return null;
    }
    return (T)Proxy.newProxyInstance(this.getClass().getClassLoader(),
        new Class[]{protocol},
        new ExecRPCInvoker(configuration,
            connection,
            protocol,
            tableName,
            row));
  }

  /**
   * <b>NO-OP for MapR Tables, returns <code>null</code>.</b><p>
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[],R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable)
      throws IOException, Throwable {
    if (maprTable_ != null) {
      return null;
    }
    final Map<byte[],R> results =  Collections.synchronizedMap(new TreeMap<byte[],R>(
        Bytes.BYTES_COMPARATOR));
    coprocessorExec(protocol, startKey, endKey, callable,
        new Batch.Callback<R>(){
      public void update(byte[] region, byte[] row, R value) {
        results.put(region, value);
      }
    });
    return results;
  }

  /**
   * <b>NO-OP for MapR Tables, returns <code>null</code>.</b><p>
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable, Batch.Callback<R> callback)
      throws IOException, Throwable {
    if (maprTable_ != null) {
      return;
    }

    // get regions covered by the row range
    List<byte[]> keys = getStartKeysInRange(startKey, endKey);
    connection.processExecs(protocol, keys, tableName, pool, callable,
        callback);
  }

  private List<byte[]> getStartKeysInRange(byte[] start, byte[] end)
  throws IOException {
    Pair<byte[][],byte[][]> startEndKeys = getStartEndKeys();
    byte[][] startKeys = startEndKeys.getFirst();
    byte[][] endKeys = startEndKeys.getSecond();

    if (start == null) {
      start = HConstants.EMPTY_START_ROW;
    }
    if (end == null) {
      end = HConstants.EMPTY_END_ROW;
    }

    List<byte[]> rangeKeys = new ArrayList<byte[]>();
    for (int i=0; i<startKeys.length; i++) {
      if (Bytes.compareTo(start, startKeys[i]) >= 0 ) {
        if (Bytes.equals(endKeys[i], HConstants.EMPTY_END_ROW) ||
            Bytes.compareTo(start, endKeys[i]) < 0) {
          rangeKeys.add(start);
        }
      } else if (Bytes.equals(end, HConstants.EMPTY_END_ROW) ||
          Bytes.compareTo(startKeys[i], end) <= 0) {
        rangeKeys.add(startKeys[i]);
      } else {
        break; // past stop
      }
    }

    return rangeKeys;
  }

  /**
   * <b>NO-OP for MapR Tables.</b><p>
   * @param operationTimeout
   */
  public void setOperationTimeout(int operationTimeout) {
    if (maprTable_ != null) {
      maprTable_.setOperationTimeout(operationTimeout);
      return;
    }
    this.operationTimeout = operationTimeout;
  }

  /**
   * <b>Returns <code>0</code> for MapR Tables.</b><p>
   */
  public int getOperationTimeout() {
    if (maprTable_ != null) {
      return maprTable_.getOperationTimeout();
    }
    return operationTimeout;
  }

}
