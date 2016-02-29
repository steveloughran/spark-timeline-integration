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

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnEventListener
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.util.Utils

/**
 * test to see that incomplete spark UIs are handled, with the UI being updated.
 */
class IncompleteSparkUISuite extends AbstractHistoryIntegrationTests with Eventually {

  override def useMiniHDFS: Boolean = true

  test("incomplete UI must not be cached") {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val connector = createUrlConnector()
      awaitHistoryRestUIListSize(connector, webUI, 0, false, TEST_STARTUP_DELAY)
      awaitHistoryRestUIListSize(connector, webUI, 0, true, TEST_STARTUP_DELAY)


      historyService = startHistoryService(sc)
      val yarnAttemptId = attemptId.toString
      val listener = new YarnEventListener(sc, historyService)

      val startTime = now()

      val started = appStartEvent(startTime,
        sc.applicationId,
        Utils.getCurrentUserName(),
        Some(yarnAttemptId))
      listener.onApplicationStart(started)
      val jobId = 2
      listener.onJobStart(jobStartEvent(startTime + 1 , jobId))
      awaitEventsProcessed(historyService, 2, 2000)
      flushHistoryServiceToSuccess()

      // listing
      awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)

      val webAttemptId = yarnAttemptId
      val webAppId = historyService.applicationId.toString

      // check for work in progress
      awaitHistoryRestUIContainsApp(connector, webUI, webAppId, false, TEST_STARTUP_DELAY)

      val attemptPath = s"/history/$webAppId/$webAttemptId"
      // GET the app
      val attemptURL = new URL(webUI, attemptPath)
      val appUIBody = connector.execHttpOperation("GET", attemptURL).responseBody
      assertContains(appUIBody, APP_NAME, s"application name in $attemptURL")
      // look for active jobs marker
      assertContains(appUIBody, activeJobsMarker, s"active jobs string in $attemptURL")

      logInfo("Ending job and application")
      // job completion event
      listener.onJobEnd(jobSuccessEvent(startTime + 1, jobId))
      // stop the app
      historyService.stop()
      awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)

      flushHistoryServiceToSuccess()

      // spin for a refresh event
      awaitRefreshExecuted(provider, true, TEST_STARTUP_DELAY)
      awaitHistoryRestUIContainsApp(connector, webUI, webAppId, true, TEST_STARTUP_DELAY)
      awaitHistoryRestUIListSize(connector, webUI, 0, false, TEST_STARTUP_DELAY)

      // the underlying timeline entity
      val entity = provider.getTimelineEntity(yarnAttemptId)

      val history = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY).head

      val historyDescription = describeApplicationHistoryInfo(history)
      assert(1 === history.attempts.size, "wrong number of app attempts ")
      val attempt1 = history.attempts.head
      assert(attempt1.completed,
        s"application attempt considered incomplete: $historyDescription")

      // get the final app UI
      val finalAppUIPage = connector.execHttpOperation("GET", attemptURL, null, "").responseBody
      assertContains(finalAppUIPage, APP_NAME, s"Application name $APP_NAME not found" +
          s" at $attemptURL")

      val stdTimeout = timeout(10 seconds)
      val stdInterval = interval(100 milliseconds)
      eventually(stdTimeout, stdInterval) {
        // the active jobs section must no longer exist
        assertDoesNotContain(finalAppUIPage, activeJobsMarker,
          s"Web UI $attemptURL still declared active in $sparkHistoryServer")

        // look for the completed job
        assertContains(finalAppUIPage, completedJobsMarker,
          s"Web UI $attemptURL does not declare completed jobs in $sparkHistoryServer")
      }

    }
    webUITest("submit and check", submitAndCheck)
  }

  /**
   * Set up base configuratin for integration tests, including
   * classname bindings in publisher & provider, refresh intervals and a port for the UI
   * @param sparkConf spark configuration
   * @return the expanded configuration
   */
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnHistoryProvider.OPTION_BACKGROUND_REFRESH_INTERVAL, "1s")
  }
}
