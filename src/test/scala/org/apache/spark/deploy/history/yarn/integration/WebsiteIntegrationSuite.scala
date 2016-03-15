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

import java.io.FileNotFoundException
import java.net.URL
import javax.ws.rs.core.MediaType

import scala.language.postfixOps

import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryService}
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.HttpOperationResponse
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.testtools.HistoryServiceListeningToSparkContext
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.util.Utils

/**
 * Complete integration test: lifecycle events through to web site
 */
class WebsiteIntegrationSuite extends AbstractHistoryIntegrationTests
    with HistoryServiceListeningToSparkContext {

  test("Get the web UI of a completed application") {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {

      addFailureAction(dumpProviderState(provider))
      addFailureAction(dumpTimelineEntities(provider))

      historyService = startHistoryService(sc)
      val listener = new YarnEventListener(sc, historyService)
      val startTime = now()

      val ctxAppId = sc.applicationId
      val started = appStartEvent(startTime, ctxAppId, Utils.getCurrentUserName())
      listener.onApplicationStart(started)
      val jobs = 10
      // run the bulk operations
      logDebug(s"Running $jobs jobs")
      for (i <- 1 to jobs) {
        sc.parallelize(1 to 10).count()
      }

      // now stop the app
      stopHistoryService(historyService)
      completed(historyService)
      val expectedAppId = historyService.applicationId.toString
      val expectedAttemptId = attemptId.toString

      // validate ATS has it
      val queryClient = createTimelineQueryClient()
      val timelineEntities = awaitSequenceSize(
        1, "applications on ATS", TIMELINE_SCAN_DELAY,
        () => queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE))
      val entry = timelineEntities.head
      assert(expectedAttemptId === entry.getEntityId,
        s"head entry id!=$expectedAttemptId: ${describeEntity(entry)} ")

      queryClient.getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, expectedAttemptId)

      // at this point the REST UI is happy. Check the provider level

      val listing = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)
      val appInListing = listing.find(_.id == expectedAppId)
      assertSome(appInListing, s"Application $expectedAppId not found in listing $listing")
      val attempts = appInListing.get.attempts
      assertNotEmpty( attempts, s"App attempts empty")
      val expectedWebAttemptId = attempts.head.attemptId.get

      // and look for the complete app
      awaitURL(webUI, TEST_STARTUP_DELAY)

      val connector = createUrlConnector()
      eventually(stdTimeout, stdInterval) {
        listRestAPIApplications(connector, webUI, true) should contain(expectedAppId)
      }

      val appPath = HistoryServer.getAttemptURI(expectedAppId, Some(expectedWebAttemptId))
      // GET the app
      val attemptURL = getAttemptURL(webUI, expectedAppId, Some(expectedWebAttemptId), "")
      logInfo(s"Fetching Application attempt from $attemptURL")
      val appUI = connector.execHttpOperation("GET", attemptURL, null, "")
      val appUIBody = appUI.responseBody
      logInfo(s"Application\n$appUIBody")
      assertContains(appUIBody, APP_NAME)

      def GET(component: String): HttpOperationResponse = {
        val url = new URL(attemptURL, s"$appPath" + component)
        logInfo(s"GET $url")
        connector.execHttpOperation("GET", url)
      }
      GET("")
      GET("/jobs")
      GET("/stages")
      GET("/storage")
      GET("/environment")
      GET("/executors")


      // then try to resolve the app on its own and expect a failure
      intercept[FileNotFoundException] {
        connector.execHttpOperation("GET", new URL(webUI, s"/history/$expectedWebAttemptId"))
      }

      val logsResponse = logs(connector, webUI, expectedAppId, expectedWebAttemptId)
      assert(logsResponse.contentType === MediaType.APPLICATION_OCTET_STREAM)
      val jobsAST = listJobsAST(connector, webUI, expectedAppId, expectedWebAttemptId)
      assertListSize(jobsAST.values, jobs, "jobs of application")

      val job0 = listJob(connector, webUI, expectedAppId, expectedWebAttemptId, 0)
      job0.stageIds.foreach { (stageId) =>
        val stageInfo = stage(connector, webUI, expectedAppId, expectedWebAttemptId, stageId)
      }

      logInfo(jsonResource(connector,
        new URL(webUI, s"${REST_BASE}/$expectedAppId/$expectedWebAttemptId/jobs")))
/* some deser problems
      val jobsList = listJobs(connector, webUI, expectedAppId, expectedWebAttemptId)
      assertListSize(jobsList, jobs, "jobs of application")
      jobsList.foreach { (jobData) =>
        logInfo(s"$jobData")
        val j = listJob(connector, webUI, expectedAppId, expectedWebAttemptId, jobData.jobId)
        j.stageIds.foreach { (stageId) =>
          val stageInfo = stage(connector, webUI, expectedAppId, expectedWebAttemptId, stageId)
        }
      }
*/

    }

    webUITest("submit and check", submitAndCheck)
  }

  /**
   * Get the full URL to an application/application attempt
   *
   * @param webUI base URL of the history server
   * @param appId application ID
   * @param attemptId attempt ID
   * @param item optional path under the URL
   * @return A URL which can be used to access the spark UI
   */
  def getAttemptURL(webUI: URL, appId: String, attemptId: Option[String], item: String = "")
    : URL = {
    val path = HistoryServer.getAttemptURI(appId, attemptId) + (if (item == "") "" else s"/$item")
    new URL(webUI, path)
  }
}
