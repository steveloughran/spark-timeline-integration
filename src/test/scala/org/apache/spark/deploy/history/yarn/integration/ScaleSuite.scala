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

import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods._

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
class ScaleSuite extends AbstractHistoryIntegrationTests
    with HistoryServiceListeningToSparkContext {

  val SCALE_TEST_JOBS = "scale.test.jobs"
  val SCALE_TEST_BATCH_SIZE = "scale.test.batch.size"
  val SCALE_TEST_QUEUE_SIZE = "scale.test.queue.size"
  val jobs = Integer.getInteger(SCALE_TEST_JOBS, 100)
  val batchSize = Integer.getInteger(SCALE_TEST_BATCH_SIZE, 200)
  val queueSize = Integer.getInteger(SCALE_TEST_QUEUE_SIZE, jobs * 20)
  val spinTimeout = (10 + jobs) * 1000

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf).set(YarnHistoryService.BATCH_SIZE, batchSize.toString)
    super.setupConfiguration(sparkConf).set(YarnHistoryService.POST_EVENT_LIMIT, queueSize.toString)
  }

  test("Scale test driven by value of " + SCALE_TEST_JOBS) {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {
      addFailureAction(dumpProviderState(provider))

      describe(s"Scale test with $SCALE_TEST_JOBS=$jobs, batch size = $batchSize," +
          s" queue size $queueSize")

      historyService = startHistoryService(sc)
      assert(historyService.listening, s"listening $historyService")
      // push in an event
      val listener = new YarnEventListener(sc, historyService)
      listener.onApplicationStart(
        appStartEvent(now(), sc.applicationId, Utils.getCurrentUserName()))
      awaitEventsProcessed(historyService, 1, TEST_STARTUP_DELAY)

      // run the bulk operations
      logDebug(s"Running $jobs jobs")
      for (i <- 1 to jobs) {
        sc.parallelize(1 to 10).count()
      }

      // now stop the app
      sc.stop()
      stopHistoryService(historyService)
      completed(historyService)
      // this is a minimum, ignoring stage events and other interim events
      val totalEventCount = 2 + jobs * 2
      val queued = historyService.metrics.eventsQueued.getCount
      assert(totalEventCount < queued)
      val posted = historyService.metrics.eventsSuccessfullyPosted.getCount
      assert(totalEventCount < posted, s"event count >= posted in $historyService")
      assert(0 === historyService.metrics.eventsDropped.getCount,
        s"Events were dropped in $historyService")

      val expectedAppId = historyService.applicationId.toString
      val expectedAttemptId = attemptId.toString

      // validate ATS has it
      val queryClient = createTimelineQueryClient()
      val timelineEntities = awaitSequenceSize(1, "applications on ATS", TIMELINE_SCAN_DELAY,
        () => queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE))
      val entry = timelineEntities.head
      assert(expectedAttemptId === entry.getEntityId,
        s"head entry id!=$expectedAttemptId: ${describeEntity(entry)} ")

      awaitEntityEventCount(queryClient, expectedAttemptId, posted, spinTimeout)

      // at this point the REST UI is happy. Check the provider level

      val listing = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)
      val appInListing = listing.find(_.id == expectedAppId)
      assertSome(appInListing, s"Application $expectedAppId not found in listing $listing")
      val attempts = appInListing.get.attempts
      assertNotEmpty(attempts, s"App attempts empty")
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

      def getJson(component: String): HttpOperationResponse = {
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

/*
  // use REST API to get #of jobs
  def getNumJobsRestful(): Int = {
    val json = HistoryServerSuite.getUrl(applications(appId, "/jobs"))
    val jsonAst = parse(json)
    val jobList = jsonAst.asInstanceOf[JArray]
    jobList.values.size
  }
*/

}
