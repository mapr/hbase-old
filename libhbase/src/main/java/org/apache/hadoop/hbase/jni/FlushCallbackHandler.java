package org.apache.hadoop.hbase.jni;

import com.stumbleupon.async.Callback;

public class FlushCallbackHandler<R, T> implements Callback<R, T> {

  private long callback;
  private long client;
  private long extra;

  public FlushCallbackHandler(long callback, long client, long extra) {
    this.callback = callback;
    this.client = client;
    this.extra = extra;
  }

  @Override
  public R call(T arg) throws Exception {
    Throwable t = null;
    if (arg instanceof Throwable) {
      t = (Throwable) arg;
    }
    CallbackHandlers.clientFlushCallBack(t,
        callback, client, extra);
    return null;
  }

}
