package org.apache.hadoop.hbase.client.mapr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class TableMappingRulesHBase implements TableMappingRulesInterface {

  public TableMappingRulesHBase(Configuration conf) {
    //  no-op constructor
  }

  @Override
  public boolean isMapRTable(byte[] tableName) throws IOException {
    return false;
  }

  @Override
  public boolean isMapRTable(String tableName) throws IOException {
    return false;
  }

  @Override
  public Path getMaprTablePath(byte[] tableName) throws IOException {
    return null;
  }

  @Override
  public Path getMaprTablePath(String tableName) throws IOException {
    return null;
  }

  @Override
  public ClusterType getClusterType() {
    return ClusterType.HBASE_ONLY;
  }

  @Override
  public byte[] isLegalTableName(byte[] tableName) {
    return HTableDescriptor.isLegalTableName(tableName);
  }

  @Override
  public String isLegalTableName(String tableName) {
    return Bytes.toString(isLegalTableName(Bytes.toBytes(tableName)));
  }

  @Override
  public boolean isMapRDefault() {
    return false;
  }

  @Override
  public Path getDefaultTablePath() throws IOException {
    return null;
  }
}
