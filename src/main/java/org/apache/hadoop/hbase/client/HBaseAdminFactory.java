package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.mapr.TableMappingRulesFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseAdminFactory extends GenericHFactory<HBaseAdminInterface> {
  private static final String DEFAULT_MAPR_IMPL = "com.mapr.fs.HBaseAdminImpl";
  private static final String DEFAULT_HBASE_IMPL = "org.apache.hadoop.hbase.client.HBaseAdminImpl";

  private static final HBaseAdminFactory INSTANCE = new HBaseAdminFactory();

  public static final HBaseAdminFactory get() {
    return INSTANCE;
  }

  public HBaseAdminInterface create(Configuration conf, String tableName) {
    return create(conf, Bytes.toBytes(tableName));
  }

  public HBaseAdminInterface create(Configuration conf, byte[] tableName) {
    return getImplementorInstance(conf, tableName,
        new Object[] {conf},
        new Class[] {Configuration.class});
  }

  public HBaseAdminInterface create(HConnection conn, String tableName) {
    return create(conn, Bytes.toBytes(tableName));
  }

  public HBaseAdminInterface create(HConnection conn, byte[] tableName) {
    return getImplementorInstance(conn.getConfiguration(), tableName,
        new Object[] {conn},
        new Class[] {HConnection.class});
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends HBaseAdminInterface> getImplementingClass(
      Configuration conf, String tableName) throws IOException {
    String engine = TableMappingRulesFactory.create(conf).isMapRTable(tableName)
          ? conf.get("hbaseadmin.impl.mapr", DEFAULT_MAPR_IMPL)
          : conf.get("hbaseadmin.impl.hbase", DEFAULT_HBASE_IMPL);

    try {
      return (Class<? extends HBaseAdminInterface>) conf.getClassByName(engine);
    }
    catch (Exception e) {
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
  }
}
