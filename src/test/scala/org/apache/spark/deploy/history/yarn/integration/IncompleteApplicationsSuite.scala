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

import org.json4s.jackson.JsonMethods

import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryService}
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.util.Utils

/**
 * Test handling/logging of incomplete applications.
 *
 * This implicitly tests some of the windowing logic. Specifically, do completed
 * applications get picked up?
 */
class IncompleteApplicationsSuite extends AbstractHistoryIntegrationTests {

  val EVENT_PROCESSED_TIMEOUT = 2000

  test("Get the web UI of an incomplete application") {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val connector = createUrlConnector()
      val incompleteURL = new URL(webUI, "/?" + incomplete_flag)
      awaitURL(incompleteURL, TEST_STARTUP_DELAY)

      historyService = startHistoryService(sc, applicationId, Some(attemptId1))
      val timeline = historyService.timelineWebappAddress
      val listener = new YarnEventListener(sc, historyService)

      listRestAPIApplications(connector, webUI, false) should have size 0
      val startTime = now()
      val expectedAppId = historyService.applicationId.toString
      val attemptId = attemptId1.toString
      val sparkAttemptId = "1"
      val started = appStartEvent(startTime,
        expectedAppId,
        Utils.getCurrentUserName(),
        Some(sparkAttemptId))
      listener.onApplicationStart(started)
      val jobId = 2
      listener.onJobStart(jobStartEvent(startTime + 1, jobId))
      awaitEventsProcessed(historyService, 2, EVENT_PROCESSED_TIMEOUT)
      flushHistoryServiceToSuccess()

      // await for a  refresh

      // listing
      awaitApplicationListingSize(provider, 1, EVENT_PROCESSED_TIMEOUT)

      val queryClient = createTimelineQueryClient()

      // check for work in progress
      val incompleteApplications = listRestAPIApplications(connector, webUI, false)
      if (incompleteApplications.size != 1) {
        fail(s"Wrong #of applications ($incompleteApplications) in "
            + JsonMethods.pretty(getJsonResource(connector, new URL(webUI, REST_APPLICATIONS))))
      }
      incompleteApplications should have size 1


      logInfo("Ending job and application")
      // job completion event
      listener.onJobEnd(jobSuccessEvent(startTime + 1, jobId))
      // stop the app
      historyService.stop()
      awaitEmptyQueue(historyService, EVENT_PROCESSED_TIMEOUT)

      // validate ATS has it
      val timelineEntities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
              primaryFilter = Some((FILTER_APP_END, FILTER_APP_END_VALUE)))
      assert(1 === timelineEntities.size, "entities listed by app end filter")
      assert(attemptId === timelineEntities.head.getEntityId,
        "attemptId === timelineEntities.head.getEntityId")

      queryClient.getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, attemptId)

      // at this point the REST UI is happy. Check the provider level

      // listing
      awaitApplicationListingSize(provider, 1, EVENT_PROCESSED_TIMEOUT)

      // and look for the complete app
      awaitURL(webUI, EVENT_PROCESSED_TIMEOUT)
      val completeBody = awaitURLDoesNotContainText(connector, webUI,
        no_completed_applications, EVENT_PROCESSED_TIMEOUT,
        s"Awaiting completed applications in the web UI listing $webUI")
      logDebug(completeBody)
      // look for the link
      assertContains(completeBody, s"$expectedAppId</a>",
        "expecting app listed in completed page")
      assertContains(completeBody, s"$expectedAppId/$sparkAttemptId",
        "expecting app attempt URL listed in completed page")

      val appPath = s"/history/$expectedAppId/$sparkAttemptId"
      // GET the app
      val appURL = new URL(webUI, appPath)
      val appUI = connector.execHttpOperation("GET", appURL, null, "")
      val appUIBody = appUI.responseBody
      logInfo(s"Application\n$appUIBody")
      assertContains(appUIBody, APP_NAME, "application name in app body")
      // look for the completed job
      assertContains(appUIBody, completedJobsMarker, "expected to list completed jobs")

      // final view has no incomplete applications
      listRestAPIApplications(connector, webUI, false) should have size 0

    }

    webUITest("submit and check", submitAndCheck)
  }

}
