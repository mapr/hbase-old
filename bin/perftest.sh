#
#/**
# * Copyright The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

function get_canonical_dir() {
  target="$1"

  canonical_name=`readlink -f ${target} 2>/dev/null`
  if [[ $? -eq 0 ]]; then
    canonical_dir=`dirname $canonical_name`
    echo ${canonical_dir}
    return
  fi

  # Mac has no readlink -f
  cd `dirname ${target}`
  target=`basename ${target}`
  # chase down the symlinks
  while [ -L ${target} ]; do
    target=`readlink ${target}`
    cd `dirname ${target}`
    target=`basename ${target}`
  done
  canonical_dir=`pwd -P`
  ret=${canonical_dir}
  echo $ret
}
bin=$(get_canonical_dir "$0")
HBASE_HOME=`cd "$bin/..">/dev/null; pwd`

#Execute hbase script to source CLASSPATH and other variables
HBASE_NOEXEC=TRUE . $HBASE_HOME/bin/hbase version &> /dev/null

if $cygwin; then
JAVA_PLATFORM=cygwin
fi
#Now we should have ${JAVA_PLATFORM} set
HBASE_NATIVE_DIR=${HBASE_HOME}/lib/native/${JAVA_PLATFORM}

HBASE_LIBRARY_PATH="$HBASE_LIBRARY_PATH:${HBASE_NATIVE_DIR}"
#Add libjvm.so's location
if [ -d "$JAVA_HOME/jre/lib/amd64/server" ]; then
  HBASE_LIBRARY_PATH="$HBASE_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server"
fi
if [ -d "$JAVA_HOME/jre/lib/i386/server" ]; then
  HBASE_LIBRARY_PATH="$HBASE_LIBRARY_PATH:$JAVA_HOME/jre/lib/i386/server"
fi
LD_LIBRARY_PATH="${HBASE_LIBRARY_PATH#:}"

#This is passed to JVM by libhbase
LIBHBASE_OPTS="${LIBHBASE_OPTS} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/ -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"

if $cygwin; then
  LD_LIBRARY_PATH=`cygpath -d "$LD_LIBRARY_PATH"`
  HBASE_NATIVE_DIR=`cygpath -d "$HBASE_NATIVE_DIR"`
fi
LIBHBASE_OPTS=${LIBHBASE_OPTS} CLASSPATH=${CLASSPATH} exec ${HBASE_NATIVE_DIR}/perftest $*
