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

package org.apache.spark.deploy.history.yarn.server

import java.io.FileNotFoundException

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.{ExitUtil, Tool, ToolRunner}
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, UnauthorizedRequestException}
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient._

/**
 * List all Spark applications in the YARN timeline server.
 *
 * If arguments are passed in, then they are accepted as attempt IDs and explicitly retrieved.
 *
 * Everything goes to stdout
 *
 * Exit codes:
 * <pre>
 *  0: success
 * 44: "not found": the endpoint or a named
 * 41: "unauthed": caller was not authenticated
 * -1: any other failure
 * </pre>
 */
class Ls extends Configured with Tool with Logging {

  /**
   * Run; args are expected to be a list of jobs.
   * @param args command line
   * @return exit code
   */
  override def run(args: Array[String]): Int = {
    exec(args)
  }

  /**
   * Execute the operation.
   * @param args list of arguments
   * @return the exit code.
   */
  def exec(args: Seq[String]): Int = {
    val yarnConf = getConf
    if (UserGroupInformation.isSecurityEnabled) {
      logInfo(s"Logging in to secure cluster as ${UserGroupInformation.getCurrentUser}")
    }
    val timelineEndpoint = getTimelineEndpoint(yarnConf)
    logInfo(s"Timeline server is at $timelineEndpoint")
    var result = 0
    var client: TimelineQueryClient = null
    try {
      client = new TimelineQueryClient(timelineEndpoint, yarnConf,
        JerseyBinding.createClientConfig())
      if (args.isEmpty) {
        client.listEntities(SPARK_EVENT_ENTITY_TYPE,
          fields = Seq(PRIMARY_FILTERS, OTHER_INFO))
            .foreach(e => logInfo(describeEntity(e)))
      } else {
        args.foreach { entity =>
          logInfo(entity)
          try {
            val tle = client.getEntity(SPARK_EVENT_ENTITY_TYPE, entity)
            logInfo(describeEntity(tle))
          } catch {
            // these inner failures can be caught and swallowed without stopping
            // the rest of the iteration
            case notFound: FileNotFoundException =>
              // one of the entities was missing: report and continue with the rest
              logInfo(s"Not found: $entity")
              result = 44

          }
        }
      }
    } catch {

      case notFound: FileNotFoundException =>
        logInfo(s"Not found: $timelineEndpoint")
        result = 44

      case ure: UnauthorizedRequestException =>
        logError(s"Authentication Failure $ure", ure)
        result = 41

      case e: Exception =>
        logError(s"Failed to fetch history", e)
        result = -1
    } finally {
      IOUtils.closeQuietly(client)
    }

    result
  }
}

object Ls {
  def main(args: Array[String]): Unit = {
    ExitUtil.halt(ToolRunner.run(new YarnConfiguration(), new Ls(), args))
  }
}

