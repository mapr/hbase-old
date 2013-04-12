package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class GenericHFactory<T> {
  protected static final Map<String, Constructor<? extends Object>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<String, Constructor<? extends Object>>();

  public Class<? extends T> getImplementingClass(
      Configuration conf, byte[] tableName) throws IOException {
    return getImplementingClass(conf, Bytes.toString(tableName));
  }

  public abstract Class<? extends T> getImplementingClass(
      Configuration conf, String tableName) throws IOException;

  @SuppressWarnings("unchecked")
  protected T getImplementorInstance(Configuration conf,
      byte[] tableName, Object[] params, Class<?>... classes) {
    StringBuffer suffix = new StringBuffer();
    if (classes != null && classes.length > 0) {
      for (Class<?> c : classes) {
        suffix.append("_").append(c.getName());
      }
    }

    Class<? extends T> clazz = null;
    try {
      clazz = getImplementingClass(conf, tableName);
      String key = clazz.getName() + suffix;
      Constructor<? extends Object> method = CONSTRUCTOR_CACHE.get(key);
      if (method == null) {
        synchronized (CONSTRUCTOR_CACHE) {
          method = CONSTRUCTOR_CACHE.get(key);
          if (method == null) {
            method = (Constructor<? extends Object>)
                      clazz.getDeclaredConstructor(classes);
            method.setAccessible(true);
            CONSTRUCTOR_CACHE.put(key, method);
          }
        }
      }
      return (T) method.newInstance(params);
    }
    catch (Throwable e) {
      throw new RuntimeException (
          "Error occurred while instantiating " +
              (clazz != null ? clazz.getName() : this.getClass().getName()) +
              "\n" + e.getMessage(), e);
    }
  }

  public static void handleIOException(Throwable t) throws IOException {
    Throwable ioe = t;
    while (ioe != null && !(ioe instanceof IOException)
        && ioe != ioe.getCause()) {
      ioe = ioe.getCause();
    }
    if (ioe == null || !(ioe instanceof IOException)) {
      throw new IOException (t);
    }
    throw (IOException) ioe;
  }
}
