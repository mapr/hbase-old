package org.apache.hadoop.hbase.client.mapr;

public class HBaseMaprMeta {
  public static String getHBaseMajorVersion() {
    return "0.94";
  }

  public static String getFilterSerializerClass() {
    return "com.mapr.fs.FilterSerializer_V1";
  }
}
