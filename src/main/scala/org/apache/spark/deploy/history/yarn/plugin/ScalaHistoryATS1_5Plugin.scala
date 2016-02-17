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

package org.apache.spark.deploy.history.yarn.plugin

import java.util

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId
import org.apache.hadoop.yarn.server.timeline.{NameValuePair, TimelineEntityGroupPlugin}
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnHistoryService._

/**
 * This is something needed in the Yarn server to decode the event stream
 */
class ScalaHistoryATS1_5Plugin extends TimelineEntityGroupPlugin with Logging {

  logInfo("Instantiated ScalaHistoryATS1_5Plugin")

  override def getTimelineEntityGroupId(
      entityType: String,
      primaryFilter: NameValuePair,
      secondaryFilters: util.Collection[NameValuePair])
    : util.Set[TimelineEntityGroupId] = {

    logDebug(s"getTimelineEntityGroupId($entityType,[$primaryFilter]")
    try {
      (entityType, primaryFilter) match {
        case (SPARK_EVENT_ENTITY_TYPE, null) =>
          null
        case (SPARK_EVENT_ENTITY_TYPE, filter) if filter.getName == FIELD_APPLICATION_ID =>
          toEntityGroupId(filter.getValue.toString)
        case (SPARK_EVENT_ENTITY_TYPE, filter) if filter.getName == FIELD_ATTEMPT_ID =>
          toGroupId(
            ConverterUtils.toApplicationAttemptId(filter.getValue.toString).getApplicationId)
        case _ =>
          null
      }
    } catch {
      case e: Exception =>
        // conversion problems, bad GET arguments, etc
        logError(s"Failed to process entity type $entityType and primary filter [$primaryFilter]")
        throw e
    }
  }


  override def getTimelineEntityGroupId(entityId: String, entityType: String)
    : util.Set[TimelineEntityGroupId] = {
    logDebug(s"getTimelineEntityGroupId($entityId, $entityType)")
    if (entityType == SPARK_EVENT_ENTITY_TYPE) {
      toGroupId(ConverterUtils.toApplicationAttemptId(entityId).getApplicationId)
    } else {
      null
    }
  }

  override def getTimelineEntityGroupId(entityType: String,
      entityIds: util.SortedSet[String],
      eventTypes: util.Set[String]): util.Set[TimelineEntityGroupId] = {
    logDebug(s"getTimelineEntityGroupId($entityType)")
    null
  }

  private def toEntityGroupId(strAppId: String): util.Set[TimelineEntityGroupId] = {
    toGroupId(ConverterUtils.toApplicationId(strAppId))
  }

  def toGroupId(applicationId: ApplicationId): util.Set[TimelineEntityGroupId] = {
    val groupId = TimelineEntityGroupId.newInstance(
      applicationId,
      SPARK_EVENT_GROUP_TYPE)
    logDebug(s"mapped $applicationId to $groupId")
    val result = new util.HashSet[TimelineEntityGroupId]()
    result.add(groupId)
    result
  }
}
