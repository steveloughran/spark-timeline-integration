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

import scala.collection.JavaConverters._

import org.apache.spark.deploy.history.yarn.YarnEventListener
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.server.TimelineApplicationAttemptInfo
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.util.Utils

/**
 * Set up a listener and feed events in; verify they get through to ATS
 */
class TimelineListenerSuite extends AbstractHistoryIntegrationTests {

  private val appStartFilter = Some((FILTER_APP_START, FILTER_APP_START_VALUE))

  private val appEndFilter = Some((FILTER_APP_END, FILTER_APP_END_VALUE))

  test("Listener Events") {
    describe("Listener events pushed out")
    // listener is still not hooked up to spark context
    historyService = startHistoryService(sc)
    val listener = new YarnEventListener(sc, historyService)
    val startTime = now()
    val contextAppId = sc.applicationId
    val started = appStartEvent(startTime, contextAppId, Utils.getCurrentUserName())
    listener.onApplicationStart(started)
    awaitEventsProcessed(historyService, 1, TEST_STARTUP_DELAY)
    stopHistoryService(historyService)
    completed(historyService)
    describe("reading events back")

    val queryClient = createTimelineQueryClient()

    // list all entries
    awaitSequenceSize(1,
      s"entities listed by timeline client $queryClient",
      TEST_STARTUP_DELAY,
      () => queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE))
    assertListSize(queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
      primaryFilter = appStartFilter),
      1,
      "entities listed by app start filter")
    assertListSize(queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
      primaryFilter = appEndFilter),
      1,
      "entities listed by app end filter")

    val timelineEntities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                                primaryFilter = appEndFilter)
    assertListSize(timelineEntities, 1, "entities listed by app end filter")
    val expectedAppId = historyService.applicationId.toString
    val expectedEntityId = attemptId.toString
    val entry = timelineEntities.head
    val entryDetails = describeEntity(entry)
    assertResult(expectedEntityId,
      s"no entity of id $expectedEntityId - found $entryDetails") {
      entry.getEntityId
    }
    queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, expectedEntityId)

    // here the events should be in the system
    val provider = new YarnHistoryProvider(sc.conf)
    val history = awaitApplicationListingSize(provider, 1, TEST_STARTUP_DELAY)
    val info = history.head
    logInfo(s"App history = $info")
    val attempt = info.attempts.head.asInstanceOf[TimelineApplicationAttemptInfo]
    // validate received data matches that saved
    assertResult(started.sparkUser, s"username in $info") {
      attempt.sparkUser
    }
    assertResult(startTime, s"started.time != startTime") {
      started.time
    }
    assertResult(started.time, s"info.startTime != started.time in $info") {
      attempt.startTime
    }
    assertResult(expectedAppId, s"info.id != expectedAppId in $info") {
      info.id
    }
    assert(attempt.endTime> 0, s"end time is ${attempt.endTime} in $info")
    // on a completed app, lastUpdated is the end time
    assert(attempt.lastUpdated >= attempt.endTime,
      s"attempt.lastUpdated  < attempt.endTime time in $info")
    assertResult(started.appName, s"info.name != started.appName in $info") {
      info.name
    }
    // fecth the Spark UI - no attempt ID
    provider.getAppUI(info.id, None)

    // hit the underlying attempt
    val timelineEntity = queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, attempt.entityId)
    val events = timelineEntity.getEvents.asScala.toList
    assertResult(2, s"number of events in ${describeEntity(timelineEntity)}") {
      events.size
    }
    // first event must be the start one
    val sparkListenerEvents = events.map(toSparkEvent).reverse
    val (firstEvent :: secondEvent :: Nil) = sparkListenerEvents
    val fetchedStartEvent = firstEvent.asInstanceOf[SparkListenerApplicationStart]
    assert(started.time === fetchedStartEvent.time, "start time")

    // direct retrieval using Spark context attempt
    queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, expectedEntityId)

  }

}
