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

import scala.collection.parallel.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore
import org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore.AppState

import org.apache.spark.Logging

/**
 * A subclass of `EntityGroupFSTimelineStore` which doesn't talk to any YARN RM for
 * application state updates; instead it looks up the value in a map; if the app
 * is not in the map then it is automatically in the unknown state
 */
class FSTimelineStoreForTesting extends EntityGroupFSTimelineStore with Logging {

  logInfo(s"YARN RM is mimiced by ${getClass.getName}")

  override def createAndInitYarnClient(conf: Configuration): YarnClient = null

  override def getAppState(appId: ApplicationId): AppState = {
    FSTimelineStoreForTesting.getAppState(appId)
  }

}

/**
 * Shared state map of applications; can be reset between tests.
 */
object FSTimelineStoreForTesting extends Logging{

  private val appStateMap = new mutable.ParHashMap[String, Boolean]

  private def liveness(b: Boolean): AppState = {
    if (b) {
      AppState.ACTIVE
    } else {
      AppState.COMPLETED
    }
  }

  def getAppState(appId: ApplicationId): AppState = {
    val state = get(appId).map(liveness).getOrElse(AppState.UNKNOWN)
    logDebug(s"$appId == $state")
    state
  }

  def get(appId: ApplicationId): Option[Boolean] = {
    appStateMap.get(appId.toString)
  }

  def put(appId: ApplicationId, live: Boolean): Unit = {
    logDebug(s"$appId -> ${liveness(live)}")
    appStateMap.put(appId.toString, live)
  }

  def remove(appId: ApplicationId): Unit = {
    logDebug(s"$appId -> ${AppState.UNKNOWN}")
    appStateMap.remove(appId.toString)
  }

  def reset(): Unit = {
    logDebug("Reset state map")
    appStateMap.clear()
  }
}
