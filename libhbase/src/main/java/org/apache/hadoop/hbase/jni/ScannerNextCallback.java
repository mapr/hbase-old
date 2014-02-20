package org.apache.hadoop.hbase.jni;

import java.util.ArrayList;

import org.hbase.async.KeyValue;

import com.stumbleupon.async.Callback;

public class ScannerNextCallback<R, T> implements Callback<R, T> {

  private long callback_;
  private long scanner_;
  private long extra_;
  private ScannerProxy proxy_;

  public ScannerNextCallback(ScannerProxy proxy, long callback, long scanner, long extra) {
    this.callback_ = callback;
    this.scanner_ = scanner;
    this.extra_ = extra;
    proxy_ = proxy;
  }

  @Override
  public R call(T arg) throws Exception {
    int numRows = 0;
    Throwable t = null;
    ResultProxy[] results = null;
    if (arg instanceof Throwable) {
      t = (Throwable) arg;
    } else if (arg != null && arg instanceof ArrayList<?>) {
      @SuppressWarnings("unchecked")
      ArrayList<ArrayList<KeyValue>> rows = (ArrayList<ArrayList<KeyValue>>) arg;
      numRows = rows.size();
      results = new ResultProxy[numRows];
      for (int i = 0; i < numRows; ++i) {
        results[i] = new ResultProxy(
            proxy_.getTable(), proxy_.getNamespace(), rows.get(i));
      }
    }
    CallbackHandlers.scanNextCallBack(t,
        callback_, scanner_, results, numRows, extra_);
    return null;
  }

}
