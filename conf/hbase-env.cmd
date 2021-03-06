@rem/**
@rem * Licensed to the Apache Software Foundation (ASF) under one
@rem * or more contributor license agreements.  See the NOTICE file
@rem * distributed with this work for additional information
@rem * regarding copyright ownership.  The ASF licenses this file
@rem * to you under the Apache License, Version 2.0 (the
@rem * "License"); you may not use this file except in compliance
@rem * with the License.  You may obtain a copy of the License at
@rem *
@rem *     http://www.apache.org/licenses/LICENSE-2.0
@rem *
@rem * Unless required by applicable law or agreed to in writing, software
@rem * distributed under the License is distributed on an "AS IS" BASIS,
@rem * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem * See the License for the specific language governing permissions and
@rem * limitations under the License.
@rem */

@rem Set environment variables here.

@rem The java implementation to use.  Java 1.6 required.
@rem set JAVA_HOME=c:\apps\java

@rem Extra Java CLASSPATH elements.  Optional.
@rem set HBASE_CLASSPATH=

@rem The maximum amount of heap to use, in MB. Default is 1000.
@rem set HBASE_HEAPSIZE=1000

@rem Extra Java runtime options.
@rem Below are what we set by default.  May only work with SUN JVM.
@rem For more on why as well as other possible settings,
@rem see http://wiki.apache.org/hadoop/PerformanceTuning
@rem JDK6 on Windows has a known bug for IPv6, use preferIPv4Stack unless JDK7.
@rem @rem See TestIPv6NIOServerSocketChannel.
set HBASE_OPTS="-XX:+UseConcMarkSweepGC" "-Djava.net.preferIPv4Stack=true"

@rem Uncomment below to enable java garbage collection logging in the .out file.
@rem set HBASE_OPTS=%HBASE_OPTS% "-verbose:gc" "-XX:+PrintGCDetails" "-XX:+PrintGCDateStamps" 

@rem Uncomment below (along with above GC logging) to put GC information in its own logfile (will set HBASE_GC_OPTS)
@rem set HBASE_USE_GC_LOGFILE=true

@rem Uncomment and adjust to enable JMX exporting
@rem See jmxremote.password and jmxremote.access in $JRE_HOME/lib/management to configure remote password access.
@rem More details at: http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
@rem
@rem set HBASE_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false" "-Dcom.sun.management.jmxremote.authenticate=false"
@rem set HBASE_MASTER_OPTS=%HBASE_JMX_BASE% "-Dcom.sun.management.jmxremote.port=10101"
@rem set HBASE_REGIONSERVER_OPTS=%HBASE_JMX_BASE% "-Dcom.sun.management.jmxremote.port=10102"
@rem set HBASE_THRIFT_OPTS=%HBASE_JMX_BASE% "-Dcom.sun.management.jmxremote.port=10103"
@rem set HBASE_ZOOKEEPER_OPTS=%HBASE_JMX_BASE% -Dcom.sun.management.jmxremote.port=10104"

@rem File naming hosts on which HRegionServers will run.  $HBASE_HOME/conf/regionservers by default.
@rem set HBASE_REGIONSERVERS=%HBASE_HOME%\conf\regionservers

@rem Where log files are stored.  $HBASE_HOME/logs by default.
@rem set HBASE_LOG_DIR=%HBASE_HOME%\logs

@rem A string representing this instance of hbase. $USER by default.
@rem set HBASE_IDENT_STRING=%USERNAME%

@rem Seconds to sleep between slave commands.  Unset by default.  This
@rem can be useful in large clusters, where, e.g., slave rsyncs can
@rem otherwise arrive faster than the master can service them.
@rem set HBASE_SLAVE_SLEEP=0.1

@rem Tell HBase whether it should manage it's own instance of Zookeeper or not.
@rem set HBASE_MANAGES_ZK=true
