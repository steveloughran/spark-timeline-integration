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

package org.apache.spark.deploy.history.yarn.integration

import java.net.URL

import scala.language.postfixOps

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.HttpOperationResponse
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.testtools.HistoryServiceListeningToSparkContext
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryService}
import org.apache.spark.util.Utils

/**
 * Scale test.
 *
 * The number of jobs to run is controlled by the system property `scale.test.jobs`, which
 * can be set in the build.
 *
 * The jobs are very small, and can overload the queues of the yarn history, so sizes of batches
 * and the total queue are expanded to cover having a large number of queued events.
 * The test will fail if the batch sizes are too small
 */
class ScaleSuiteATSv1 extends ScaleSuite {

  override def enableATSv15: Boolean = false
}
