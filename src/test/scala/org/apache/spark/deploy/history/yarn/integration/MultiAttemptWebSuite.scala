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

import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient._
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * Create a completed app from multiple app attempts and fetch from Web UI
 */
class MultiAttemptWebSuite extends AbstractHistoryIntegrationTests {

  override def useMiniHDFS: Boolean = true

  test("Multi-attempt web UI") {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {

      postMultipleAttempts()
      val queryClient = createTimelineQueryClient()
      val conf = sc.hadoopConfiguration
      stopContextAndFlushHistoryService()

      val expectedAppId = historyService.applicationId.toString
      val timelineEntities = awaitEntityListSize(queryClient, 2)

      val head = timelineEntities.head
      val attempt1 = attemptId1.toString
      val attempt2 = attemptId2.toString
      assert(attempt1 === head.getEntityId || attempt2 === head.getEntityId,
        s"wrong entity id in ${describeEntity(head)}")

      queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, attempt1)
      queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, attempt2)

      // at this point the ATS REST API is happy. Check the provider level

      // listing must eventually contain two attempts
      val appHistory = awaitListingEntry(provider, expectedAppId, 2, TEST_STARTUP_DELAY)
      val historyDescription = describeApplicationHistoryInfo(appHistory)
      // check the provider thinks that it has completed
      assert(isCompleted(appHistory), s"App is not completed $historyDescription")

      // resolve to entries
      getAppUI(provider, expectedAppId, Some(attempt1SparkId))
      getAppUI(provider, expectedAppId, Some(attempt2SparkId))

      // then look for the complete app on the web
      awaitURL(webUI, TEST_STARTUP_DELAY)

      describe("Awaiting REST UI to show app")
      val connector = createUrlConnector(conf)
      awaitHistoryRestUIListSize(connector, webUI, 1, true, TEST_STARTUP_DELAY)
      val appPath = s"/history/$expectedAppId/$attempt1SparkId"
      // GET the app
      val appURL = new URL(webUI, appPath)
      val appUI = connector.execHttpOperation("GET", appURL, null, "")
      val appUIBody = appUI.responseBody
      logInfo(s"Application\n$appUIBody")
      assertContains(appUIBody, APP_NAME)
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/jobs"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/stages"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/storage"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/environment"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/executors"), null, "")

      describe("looking at REST UI")
      awaitHistoryRestUIContainsApp(connector, webUI, expectedAppId, true, TEST_STARTUP_DELAY)
    }

    webUITest("submit and check", submitAndCheck)
  }

}
