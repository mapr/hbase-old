package org.apache.hadoop.hbase.client.mapr;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This class defines public methods implemented in {@link HTable} class but not
 * defined in {@link HTableInterface}.
 *
 * It also contains default or No-OP implementations of HTableInterface methods
 * which are not implemented for MapR tables.
 */
public abstract class AbstractHTable implements HTableInterface {

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    batchEx(actions, results);
  }
  public abstract void batchEx(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException;
  
  @Override
  public Object[] batch(List<? extends Row> actions)
      throws IOException, InterruptedException {
    return batchEx(actions);
  }
  public abstract Object[] batchEx(List<? extends Row> actions)
      throws IOException, InterruptedException;
  
  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }


  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return The location of the given row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final String row) throws IOException {
    return getRegionLocation(Bytes.toBytes(row));
  }

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
  public HRegionLocation getRegionLocation(byte[] row, boolean reload)
      throws IOException {
    return getRegionLocation(row);
  }

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte[][] getStartKeys() throws IOException {
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
  public abstract Pair<byte[][],byte[][]> getStartEndKeys() throws IOException;

  /**
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public abstract NavigableMap<HRegionInfo, ServerName> getRegionLocations()
      throws IOException;

  /** {@inheritDoc} */
  @Override
  public long getWriteBufferSize() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    /* NO-OP */
  }

  /** {@inheritDoc} */
  @Override
  public RowLock lockRow(byte[] row) throws IOException {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void unlockRow(RowLock rl) throws IOException {
  }

  /** {@inheritDoc} */
  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol,
      byte[] row) {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey, Call<T, R> callable)
      throws IOException, Throwable {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey, Call<T, R> callable,
      Callback<R> callback) throws IOException, Throwable {
  }

  public void clearRegionCache() { }

  public int getOperationTimeout() { return 0; }

  public void setOperationTimeout(int operationTimeout) { }
}
