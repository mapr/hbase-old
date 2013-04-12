package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Pair;

public abstract interface AbstractHTableInterface extends Closeable {

  /**
   * Gets the name of this table.
   *
   * @return the table name.
   */
  byte[] getTableName();

  /**
   * Returns the {@link Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will
   * affect this instance.
   */
  Configuration getConfiguration();

  /**
   * Gets the {@link HTableDescriptor table descriptor} for this table.
   * @throws IOException if a remote or network exception occurs.
   */
  HTableDescriptor getTableDescriptor() throws IOException;

  /**
   * Test for the existence of columns in the table, as specified in the Get.
   * <p>
   *
   * This will return true if the Get matches one or more keys, false if not.
   * <p>
   *
   * This is a server-side call so it prevents any data from being transfered to
   * the client.
   *
   * @param get the Get
   * @return true if the specified Get matches one or more keys, false if not
   * @throws IOException e
   */
  boolean exists(Get get) throws IOException;

  /**
   * Method that does a batch call on Deletes, Gets and Puts. The ordering of
   * execution of the actions is not defined. Meaning if you do a Put and a
   * Get in the same {@link #batch} call, you will not necessarily be
   * guaranteed that the Get returns what the Put had put.
   *
   * @param actions list of Get, Put, Delete objects
   * @param results Empty Object[], same size as actions. Provides access to partial
   *                results, in case an exception is thrown. A null in the result array means that
   *                the call for that action failed, even after retries
   * @throws IOException
   * @since 0.90.0
   */
  void batch(final List<? extends Row> actions, final Object[] results) throws IOException, InterruptedException;

  /**
   * Same as {@link #batch(List, Object[])}, but returns an array of
   * results instead of using a results parameter reference.
   *
   * @param actions list of Get, Put, Delete objects
   * @return the results from the actions. A null in the return array means that
   *         the call for that action failed, even after retries
   * @throws IOException
   * @since 0.90.0
   */
  Object[] batch(final List<? extends Row> actions) throws IOException, InterruptedException;

  /**
   * Extracts certain cells from a given row.
   * @param get The object that specifies what data to fetch and from which row.
   * @return The data coming from the specified row, if it exists.  If the row
   * specified doesn't exist, the {@link Result} instance returned won't
   * contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  Result get(Get get) throws IOException;

  /**
   * Extracts certain cells from the given rows, in batch.
   *
   * @param gets The objects that specify what data to fetch and from which rows.
   *
   * @return The data coming from the specified rows, if it exists.  If the row
   *         specified doesn't exist, the {@link Result} instance returned won't
   *         contain any {@link KeyValue}, as indicated by {@link Result#isEmpty()}.
   *         If there are any failures even after retries, there will be a null in
   *         the results array for those Gets, AND an exception will be thrown.
   * @throws IOException if a remote or network exception occurs.
   *
   * @since 0.90.0
   */
  Result[] get(List<Get> gets) throws IOException;

  /**
   * Return the row that matches <i>row</i> exactly,
   * or the one that immediately precedes it.
   *
   * @param row A row key.
   * @param family Column family to include in the {@link Result}.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   *
   * @deprecated As of version 0.92 this method is deprecated without
   * replacement.
   * getRowOrBefore is used internally to find entries in .META. and makes
   * various assumptions about the table (which are true for .META. but not
   * in general) to be efficient.
   */
  Result getRowOrBefore(byte[] row, byte[] family) throws IOException;

  /**
   * Returns a scanner on the current table as specified by the {@link Scan}
   * object.
   *
   * @param scan A configured {@link Scan} object.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  ResultScanner getScanner(Scan scan) throws IOException;

  /**
   * Gets a scanner on the current table for the given family.
   *
   * @param family The column family to scan.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  ResultScanner getScanner(byte[] family) throws IOException;

  /**
   * Gets a scanner on the current table for the given family and qualifier.
   *
   * @param family The column family to scan.
   * @param qualifier The column qualifier to scan.
   * @return A scanner.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException;


  /**
   * Puts some data in the table.
   * <p>
   * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
   * until the internal buffer is full.
   * @param put The data to put.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  void put(Put put) throws IOException;

  /**
   * Puts some data in the table, in batch.
   * <p>
   * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
   * until the internal buffer is full.
   * <p>
   * This can be used for group commit, or for submitting user defined
   * batches.  The writeBuffer will be periodically inspected while the List
   * is processed, so depending on the List size the writeBuffer may flush
   * not at all, or more than once.
   * @param puts The list of mutations to apply. The batch put is done by
   * aggregating the iteration of the Puts over the write buffer
   * at the client-side for a single RPC call.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  void put(List<Put> puts) throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the put.  If the passed value is null, the check
   * is for the lack of column (ie: non-existance)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   * @return true if the new put was executed, false otherwise
   */
  boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException;

  /**
   * Deletes the specified cells/row.
   *
   * @param delete The object that specifies what to delete.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  void delete(Delete delete) throws IOException;

  /**
   * Deletes the specified cells/rows in bulk.
   * @param deletes List of things to delete.  List gets modified by this
   * method (in particular it gets re-ordered, so the order in which the elements
   * are inserted in the list gives no guarantee as to the order in which the
   * {@link Delete}s are executed).
   * @throws IOException if a remote or network exception occurs. In that case
   * the {@code deletes} argument will contain the {@link Delete} instances
   * that have not be successfully applied.
   * @since 0.20.1
   */
  void delete(List<Delete> deletes) throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.  If the passed value is null, the
   * check is for the lack of column (ie: non-existance)
   *
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @throws IOException e
   * @return true if the new delete was executed, false otherwise
   */
  boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException;

  /**
   * Performs multiple mutations atomically on a single row. Currently
   * {@link Put} and {@link Delete} are supported.
   *
   * @param arm object that specifies the set of mutations to perform
   * atomically
   * @throws IOException
   */
  public void mutateRow(final RowMutations rm) throws IOException;

  /**
   * Appends values to one or more columns within a single row.
   * <p>
   * This operation does not appear atomic to readers.  Appends are done
   * under a single row lock, so write operations to a row are synchronized, but
   * readers do not take row locks so get and scan operations can see this
   * operation partially completed.
   *
   * @param append object that specifies the columns and amounts to be used
   *                  for the increment operations
   * @throws IOException e
   * @return values of columns after the append operation (maybe null)
   */
  public Result append(final Append append) throws IOException;

  /**
   * Increments one or more columns within a single row.
   * <p>
   * This operation does not appear atomic to readers.  Increments are done
   * under a single row lock, so write operations to a row are synchronized, but
   * readers do not take row locks so get and scan operations can see this
   * operation partially completed.
   *
   * @param increment object that specifies the columns and amounts to be used
   *                  for the increment operations
   * @throws IOException e
   * @return values of columns after the increment
   */
  public Result increment(final Increment increment) throws IOException;

  /**
   * Atomically increments a column value.
   * <p>
   * Equivalent to {@link #incrementColumnValue(byte[], byte[], byte[],
   * long, boolean) incrementColumnValue}(row, family, qualifier, amount,
   * <b>true</b>)}
   * @param row The row that contains the cell to increment.
   * @param family The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount The amount to increment the cell with (or decrement, if the
   * amount is negative).
   * @return The new value, post increment.
   * @throws IOException if a remote or network exception occurs.
   */
  long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException;

  /**
   * Atomically increments a column value. If the column value already exists
   * and is not a big-endian long, this could throw an exception. If the column
   * value does not yet exist it is initialized to <code>amount</code> and
   * written to the specified column.
   *
   * <p>Setting writeToWAL to false means that in a fail scenario, you will lose
   * any increments that have not been flushed.
   * @param row The row that contains the cell to increment.
   * @param family The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount The amount to increment the cell with (or decrement, if the
   * amount is negative).
   * @param writeToWAL if {@code true}, the operation will be applied to the
   * Write Ahead Log (WAL).  This makes the operation slower but safer, as if
   * the call returns successfully, it is guaranteed that the increment will
   * be safely persisted.  When set to {@code false}, the call may return
   * successfully before the increment is safely persisted, so it's possible
   * that the increment be lost in the event of a failure happening before the
   * operation gets persisted.
   * @return The new value, post increment.
   * @throws IOException if a remote or network exception occurs.
   */
  long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException;

  /**
   * Tells whether or not 'auto-flush' is turned on.
   *
   * @return {@code true} if 'auto-flush' is enabled (default), meaning
   * {@link Put} operations don't get buffered/delayed and are immediately
   * executed.
   */
  boolean isAutoFlush();

  /**
   * Executes all the buffered {@link Put} operations.
   * <p>
   * This method gets called once automatically for every {@link Put} or batch
   * of {@link Put}s (when <code>put(List<Put>)</code> is used) when
   * {@link #isAutoFlush} is {@code true}.
   * @throws IOException if a remote or network exception occurs.
   */
  void flushCommits() throws IOException;

  /**
   * Releases any resources help or pending changes in internal buffers.
   *
   * @throws IOException if a remote or network exception occurs.
   */
  void close() throws IOException;

  /**
   * Obtains a lock on a row.
   *
   * @param row The row to lock.
   * @return A {@link RowLock} containing the row and lock id.
   * @throws IOException if a remote or network exception occurs.
   * @see RowLock
   * @see #unlockRow
   */
  RowLock lockRow(byte[] row) throws IOException;

  /**
   * Releases a row lock.
   *
   * @param rl The row lock to release.
   * @throws IOException if a remote or network exception occurs.
   * @see RowLock
   * @see #unlockRow
   */
  void unlockRow(RowLock rl) throws IOException;

  /**
   * Creates and returns a proxy to the CoprocessorProtocol instance running in the
   * region containing the specified row.  The row given does not actually have
   * to exist.  Whichever region would contain the row based on start and end keys will
   * be used.  Note that the {@code row} parameter is also not passed to the
   * coprocessor handler registered for this protocol, unless the {@code row}
   * is separately passed as an argument in a proxy method call.  The parameter
   * here is just used to locate the region used to handle the call.
   *
   * @param protocol The class or interface defining the remote protocol
   * @param row The row key used to identify the remote region location
   * @return A CoprocessorProtocol instance
   */
  <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row);

  /**
   * Invoke the passed
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call} against
   * the {@link CoprocessorProtocol} instances running in the selected regions.
   * All regions beginning with the region containing the <code>startKey</code>
   * row, through to the region containing the <code>endKey</code> row (inclusive)
   * will be used.  If <code>startKey</code> or <code>endKey</code> is
   * <code>null</code>, the first and last regions in the table, respectively,
   * will be used in the range selection.
   *
   * @param protocol the CoprocessorProtocol implementation to call
   * @param startKey start region selection with region containing this row
   * @param endKey select regions up to and including the region containing
   * this row
   * @param callable wraps the CoprocessorProtocol implementation method calls
   * made per-region
   * @param <T> CoprocessorProtocol subclass for the remote invocation
   * @param <R> Return type for the
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
   * method
   * @return a <code>Map</code> of region names to
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)} return values
   */
  <T extends CoprocessorProtocol, R> Map<byte[],R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey, Batch.Call<T,R> callable)
      throws IOException, Throwable;

  /**
   * Invoke the passed
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call} against
   * the {@link CoprocessorProtocol} instances running in the selected regions.
   * All regions beginning with the region containing the <code>startKey</code>
   * row, through to the region containing the <code>endKey</code> row
   * (inclusive)
   * will be used.  If <code>startKey</code> or <code>endKey</code> is
   * <code>null</code>, the first and last regions in the table, respectively,
   * will be used in the range selection.
   *
   * <p>
   * For each result, the given
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[], byte[], Object)}
   * method will be called.
   *</p>
   *
   * @param protocol the CoprocessorProtocol implementation to call
   * @param startKey start region selection with region containing this row
   * @param endKey select regions up to and including the region containing
   * this row
   * @param callable wraps the CoprocessorProtocol implementation method calls
   * made per-region
   * @param callback an instance upon which
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[], byte[], Object)} with the
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
   * return value for each region
   * @param <T> CoprocessorProtocol subclass for the remote invocation
   * @param <R> Return type for the
   * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
   * method
   */
  <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable, Batch.Callback<R> callback)
      throws IOException, Throwable;

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return The location of the given row.
   * @throws IOException if a remote or network exception occurs
   */
  public abstract HRegionLocation getRegionLocation(final String row)
      throws IOException;

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link #getRegionLocation(byte [], boolean)} instead
   */
  public abstract HRegionLocation getRegionLocation(final byte[] row)
      throws IOException;

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @param reload whether or not to reload information or just use cached
   * information
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  public abstract HRegionLocation getRegionLocation(byte[] row, boolean reload)
      throws IOException;

  /**
   * <em>INTERNAL</em> Used by unit tests and tools to do low-level
   * manipulations.
   * @return An HConnection instance.
   * @deprecated This method will be changed from public to package protected.
   */
  public abstract HConnection getConnection();

  /**
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
   * @deprecated Use {@link Scan#setCaching(int)} and {@link Scan#getCaching()}
   */
  public abstract int getScannerCaching();

  /**
   * Sets the number of rows that a scanner will fetch at once.
   * <p>
   * This will override the value specified by
   * {@code hbase.client.scanner.caching}.
   * Increasing this value will reduce the amount of work needed each time
   * {@code next()} is called on a scanner, at the expense of memory use
   * (since more rows will need to be maintained in memory by the scanners).
   * @param scannerCaching the number of rows a scanner will fetch at once.
   * @deprecated Use {@link Scan#setCaching(int)}
   */
  public abstract void setScannerCaching(int scannerCaching);

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  public abstract byte[][] getStartKeys() throws IOException;

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public abstract byte[][] getEndKeys() throws IOException;

  /**
   * Gets the starting and ending row keys for every region in the currently
   * open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public abstract Pair<byte[][],byte[][]> getStartEndKeys() throws IOException;

  /**
   * Gets all the regions and their address for this table.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link #getRegionLocations()} or {@link #getStartEndKeys()}
   */
  public abstract Map<HRegionInfo, HServerAddress> getRegionsInfo()
      throws IOException;

  /**
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public abstract NavigableMap<HRegionInfo, ServerName> getRegionLocations()
      throws IOException;

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
  public abstract void prewarmRegionCache(Map<HRegionInfo,
      HServerAddress> regionMap);

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
  public abstract void serializeRegionInfo(DataOutput out) throws IOException;

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
  public abstract Map<HRegionInfo, HServerAddress> deserializeRegionInfo(
      DataInput in) throws IOException;

  /**
   * See {@link #setAutoFlush(boolean, boolean)}
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   */
  public abstract void setAutoFlush(boolean autoFlush);

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
  public abstract void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail);

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter
   * {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  public abstract long getWriteBufferSize();

  /**
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the
   * write buffer, the buffer gets flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   */
  public abstract void setWriteBufferSize(long writeBufferSize) throws IOException;

  /**
   * Returns the write buffer.
   * @return The current write buffer.
   */
  public abstract ArrayList<Put> getWriteBuffer();

  /**
   * Explicitly clears the region cache to fetch the latest value from META.
   * This is a power user function: avoid unless you know the ramifications.
   */
  public abstract void clearRegionCache();

  public abstract void setOperationTimeout(int operationTimeout);
  public abstract int getOperationTimeout();

  /**
   * The pool is used for mutli requests for this HTable
   * @return the pool used for mutli
   */
  public abstract ExecutorService getPool();

  /**
   * Get the corresponding regions for an arbitrary range of keys.
   * <p>
   * @param startRow Starting row in range, inclusive
   * @param endRow Ending row in range, exclusive
   * @return A list of HRegionLocations corresponding to the regions that
   * contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  public abstract List<HRegionLocation> getRegionsInRange(byte[] startKey,
      byte[] endKey) throws IOException;

}
