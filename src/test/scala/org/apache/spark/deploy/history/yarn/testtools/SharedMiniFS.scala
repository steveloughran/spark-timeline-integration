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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, FileSystem, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
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

  /**
    * Retrieve or create the cluster.
    * If the cluster existed, delete all its data.
    * If it does not exist, create it in the directory `${project.build.dir}/minicluster`
    * (falling back to `java.io.tmpdir` if the system property `project.build.dir` is unset.
    *
    * @param conf configuration to use if creating a new cluster.
    */
  def retrieve(conf: Configuration): FileSystem = {

    if (cluster.isDefined) {
      // erase the cluster
      val fs = getFileSystem()
      fs.delete(new Path("/"), true)
    } else {
      // set up the directory path
      val projectBuildDir = new File(System.getProperty("project.build.dir",
        System.getProperty("java.io.tmpdir")))
      val miniclusterDir = new File(projectBuildDir, "minicluster")
      FileUtil.fullyDelete(miniclusterDir)
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, miniclusterDir.getAbsolutePath)
      // turn off metrics log noise
      conf.setInt("dfs.datanode.metrics.logger.period.seconds", 0)
      conf.setInt("dfs.namenode.metrics.logger.period.seconds", 0)
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
