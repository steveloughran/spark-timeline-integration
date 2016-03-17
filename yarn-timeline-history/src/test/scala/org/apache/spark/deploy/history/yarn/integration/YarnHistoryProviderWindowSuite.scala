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

import org.apache.hadoop.yarn.api.records.YarnApplicationState

import org.apache.spark.deploy.history.yarn.server.TimelineApplicationHistoryInfo
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.deploy.history.yarn.testtools.{HistoryServiceNotListeningToSparkContext, TimelineSingleEntryBatchSize}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.util.Utils

/**
 * check windowed providder.
 *
 * More than one history service is started here, each publishing their own events, with
 * their own app ID. For this to work they are set up to not listen to context events.
 *
 * The two apps are launched such that the first launched app is not yet completed before the
 * second.
 */
class YarnHistoryProviderWindowSuite
    extends AbstractHistoryIntegrationTests
    with HistoryServiceNotListeningToSparkContext
    with TimelineSingleEntryBatchSize {
  val minute = 60000
  val start1Time = minute
  val start2Time = start1Time + minute
  val appReport1 = stubApplicationReport(1, 0, 1, YarnApplicationState.RUNNING, start1Time, 0)
  val appReport2 = stubApplicationReport(2, 0, 1, YarnApplicationState.RUNNING, start2Time, 0)

  val appId1 = appReport1.getApplicationId.toString
  val appId2 = appReport2.getApplicationId.toString
  val user = Utils.getCurrentUserName()

  override def useMiniHDFS: Boolean = true

  /**
   * Verifies that window tracking doesn't ever move the scan window after an incomplete app.
   * That is: it blocks at the last (running) incomplete app in the listing, even
   * after completed ones come in after.
   */
  test("YarnHistoryProviderWindow") {
    describe("Windowed publishing across apps")
    var history2: YarnHistoryService = null
    var provider: TimeManagedHistoryProvider = null
    try {
      logDebug("Start application 1")
      val expectedAppId1 = appReport1.getApplicationId
      val attemptId1 = appReport1.getCurrentApplicationAttemptId
      historyService = startHistoryService(sc, expectedAppId1,
        Some(appReport1.getCurrentApplicationAttemptId))
      assert(!historyService.listening, s"listening $historyService")
      assert(historyService.bondedToATS, s"not bonded to ATS: $historyService")
      // post in an app start
      val start1 = appStartEvent(start1Time, appId1, user, Some(attemptId1.toString))

      enqueue(start1)
      flushHistoryServiceToSuccess(historyService)

      // a new application is started before the current history is started
      describe("application 2")
      // the second application starts then stops after the first one
      val applicationId2 = appReport2.getApplicationId
      val attemptId2 = appReport2.getCurrentApplicationAttemptId
      val expectedAppId2 = applicationId2.toString
      history2 = startHistoryService(sc, applicationId2,
      Some(appReport2.getCurrentApplicationAttemptId))

      history2.process(appStartEvent(start2Time, appId2, user, Some(attemptId2.toString)))
      val end2Time = start2Time + minute
      history2.process(appStopEvent(end2Time))
      // stop the second application
      stopHistoryService(history2)
      completed(history2)
      addFailureAction(failureLog(s"History Service 2: $history2"))

      // here there is one incomplete application, and a completed one
      // which started and stopped after the incomplete one started
      provider = new TimeManagedHistoryProvider(sc.conf, end2Time, minute)
      provider.setRunningApplications(List(appReport1, appReport2))
      addFailureAction(failureLog(s"History Service 1: $historyService"))
      addFailureAction(dumpProviderState(provider))
      addFailureAction(dumpTimelineEntities(provider))

      // now read it in via history provider
      describe("read in listing")

      // An eventually{} clause as there's a risk that the initial provider cache refresh
      // takes place before ATS has picked up the completed event.
      var listing1: Seq[TimelineApplicationHistoryInfo] = null
      eventually(stdTimeout, stdInterval) {
        listing1 = awaitApplicationListingSize(provider, 2, TEST_STARTUP_DELAY)
        logInfo(s"Listing 1: $listing1")
        assertAppCompleted(lookupApplication(listing1, expectedAppId2),
          s"app2 ID $expectedAppId2, in listing1 $listing1")
      }
      val applicationInfo1_1 = lookupApplication(listing1, expectedAppId1)
      assert(!isCompleted(applicationInfo1_1), s"$applicationInfo1_1 completed in L1 $listing1")

      describe("stop application 1")
      historyService.process(appStopEvent(provider.tick()))
      stopHistoryService(historyService)
      completed(historyService)


      // query history service direct for the app, by listing entities and
      // asserting that one is valid
      val queryClient = createTimelineQueryClient()
      eventually(stdTimeout, stdInterval) {
        val entities = listEntities(queryClient)
        val entityNames = entities.map(_.getEntityId).mkString(" ")
        val maybeEntity1 = entities.find(_.getEntityId == attemptId1.toString)
        assertSome(maybeEntity1, s"Did not find attempt $attemptId1 in $entityNames")
        val entity = maybeEntity1.get
        val appHistoryApp1Attempt1 = toApplicationHistoryInfo(entity)
        assert (appHistoryApp1Attempt1.completed,
          s"App never completed; history=$appHistoryApp1Attempt1," +
          s"entity=${describeEntity(entity)}")
      }

      // move time forwards
      provider.incrementTime(5 * minute)
      // Now await a refresh
      describe("read in listing #2")

      awaitRefreshExecuted(provider, true, TEST_STARTUP_DELAY)
      awaitRefreshExecuted(provider, true, TEST_STARTUP_DELAY)
      awaitRefreshExecuted(provider, true, TEST_STARTUP_DELAY)

      logDebug("Refreshes executed; extracting application listing")
      val allApps = provider.listApplications()
      logInfo(s"allApps : ${allApps.applications}")

      // get a new listing
      val listing2 = provider.getListing()
      logInfo(s"Listing 2: $listing2")
      // which had better be updated or there are refresh problems
      eventually(stdTimeout, stdInterval) {
        assert(listing1 !== listing2, s"updated listing was unchanged from $provider")
      }

      // get the updated value and expect it to be complete
      assertAppCompleted(lookupApplication(listing2, expectedAppId1), s"app1 in L2 $listing2")
      assertAppCompleted(lookupApplication(listing2, expectedAppId1), s"app2 in L2 $listing2")
      provider.stop()
    } catch {
      case ex: Exception =>
        executeFailureActions()
        throw ex
    } finally {
      describe("teardown")
      if (history2 != null) {
        history2.stop()
      }
      if (provider != null) {
        provider.stop()
      }
    }
  }

}
