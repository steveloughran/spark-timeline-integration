/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history.yarn.testtools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{DFSConfigKeys, MiniDFSCluster}
import org.apache.hadoop.hdfs.MiniDFSCluster.Builder

import org.apache.spark.Logging

/**
 * This provides access to shared mini HDFS cluster.
 *
 * 1. It is created on the first attempt to retrieve the cluster, using
 * the configuration passed in. This is not a thread-safe operation.
 * 2. It is re-used on future requests.
 * 3. When re-using it, the filesystem is erased.
 * 4. Therefore: don't attempt to use it in parallel.
 * 5. The cluster is not destroyed at the end of any test run, as it is not
 * possible to determine whether the cluster will be needed afterwards.
 */
object SharedMiniFS extends Logging {

  var cluster: Option[MiniDFSCluster] = None

  def retrieve(conf: Configuration): FileSystem = {

    if (cluster.isDefined) {
      // erase the cluster
      val fs = getFileSystem()
      fs.delete(new Path("/"), true)
    } else {
      conf.setInt(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, 0)
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, 0)
      val builder = new Builder(conf)
      cluster = Some(builder.build())
      logInfo(s"Started MiniHDFS at ${cluster.get.getURI}")
    }
    getFileSystem()
  }

  private def getFileSystem() : FileSystem = {
    cluster.get.getFileSystem()
  }
}
