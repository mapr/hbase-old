/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.mapr;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class TableMappingRulesFactory {
  private static final String DEFAULT_MAPR_IMPL = "com.mapr.fs.TableMappingRules";
  private static final String FALLBACK_MAPR_IMPL = 
      "org.apache.hadoop.hbase.client.mapr.TableMappingRulesHBase";

  private static final Log LOG = LogFactory.getLog(TableMappingRulesFactory.class);

  private static volatile boolean hbaseOnly = false;
  private static volatile Constructor<? extends TableMappingRulesInterface> constructor_ = null;

  /**
   * @param conf
   * @return
   * @throws IOException
   */
  public static TableMappingRulesInterface create(Configuration conf) 
      throws IOException {
    try {
      Constructor<? extends TableMappingRulesInterface> method = constructor_;
      if (method == null) {
        synchronized (TableMappingRulesFactory.class) {
          method = constructor_;
          if (method == null) {
            String impl = conf.get("hbase.mappingrule.impl", DEFAULT_MAPR_IMPL);
            try {
              @SuppressWarnings("unchecked")
              Class<? extends TableMappingRulesInterface> clazz = 
                  (Class<? extends TableMappingRulesInterface>) conf
                  .getClassByName(impl);
              constructor_ = method = (Constructor<? extends TableMappingRulesInterface>) clazz
                  .getDeclaredConstructor(Configuration.class);
            } catch (ClassNotFoundException e) {
              hbaseOnly = true;
              LOG.info("Could not instantiate TableMappingRules class, assuming HBase only cluster.");
              LOG.info("'mapr-hbase-dbclient' package is required to access MapRDB tables.");
              LOG.debug(e.getMessage(), e);

              impl = FALLBACK_MAPR_IMPL;
              @SuppressWarnings("unchecked")
              Class<? extends TableMappingRulesInterface> clazz = 
                  (Class<? extends TableMappingRulesInterface>) conf
                  .getClassByName(impl);
              constructor_ = method = (Constructor<? extends TableMappingRulesInterface>) clazz
                  .getDeclaredConstructor(Configuration.class);
            }
          }
        }
      }
      return (TableMappingRulesInterface) method.newInstance(conf);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static boolean isHbaseOnly() {
    return hbaseOnly;
  }
}
