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

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore.AppState

import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.cluster.StubApplicationId

class TimelineStoreSuite extends AbstractHistoryIntegrationTests {

  test("FSTimelineStoreForTesting liveness") {
    val store = new FSTimelineStoreForTesting()
    store.getAppState(applicationId) should be(AppState.UNKNOWN)
    FSTimelineStoreForTesting.put(applicationId, true)
    store.getAppState(applicationId) should be(AppState.ACTIVE)
    FSTimelineStoreForTesting.put(applicationId, false)
    store.getAppState(applicationId) should be(AppState.COMPLETED)
    FSTimelineStoreForTesting.remove(applicationId)
    store.getAppState(applicationId) should be(AppState.UNKNOWN)
    started(applicationId)
    store.getAppState(applicationId) should be(AppState.ACTIVE)
    completed(applicationId)
    store.getAppState(applicationId) should be(AppState.COMPLETED)
    FSTimelineStoreForTesting.reset()
    store.getAppState(applicationId) should be(AppState.UNKNOWN)

    FSTimelineStoreForTesting.put(applicationId, true)
    store.getAppState(applicationId) should be(AppState.ACTIVE)

    val applicationId2= new StubApplicationId(0, 1111L)
    store.getAppState(applicationId2) should be(AppState.ACTIVE)

  }

}
