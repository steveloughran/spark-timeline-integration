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

package org.apache.spark.deploy.history.yarn

/**
 * All the constants for the history integration.
 *
 * Some of these need to be kept in sync with constants in the `SparkATSPlugin` class;
 * changing the fields will also break all existing history lookups.
 */
class EntityConstants {
  /**
   * Name of the entity type used to declare spark Applications.
   */
  val SPARK_EVENT_ENTITY_TYPE: String = "spark_event_v01"
  /**
   * Name of the entity type used to declare spark Applications.
   */
  val SPARK_EVENT_GROUP_TYPE: String = "spark_event_group_v01"
  /**
   * Domain ID.
   */
  val DOMAIN_ID_PREFIX: String = "Spark_ATS_"
  /**
   * Primary key used for events
   */
  val PRIMARY_KEY: String = "spark_application_entity"
  /**
   *  Entity `OTHER_INFO` field: start time
   */
  val FIELD_START_TIME: String = "startTime"
  /**
   * Entity `OTHER_INFO` field: last updated time.
   */
  val FIELD_LAST_UPDATED: String = "lastUpdated"
  /**
   * Entity `OTHER_INFO` field: end time. Not present if the app is running.
   */
  val FIELD_END_TIME: String = "endTime"
  /**
   * Entity `OTHER_INFO` field: application name from context.
   */
  val FIELD_APP_NAME: String = "appName"
  /**
   * Entity `OTHER_INFO` field: user.
   */
  val FIELD_APP_USER: String = "appUser"
  /**
   * Entity `OTHER_INFO` field: YARN application ID.
   */
  val FIELD_APPLICATION_ID: String = "applicationId"
  /**
   * Entity `OTHER_INFO` field: attempt ID from spark start event.
   */
  val FIELD_ATTEMPT_ID: String = "attemptId"
  /**
   * Entity `OTHER_INFO` field: a counter which is incremented whenever a new timeline entity
   * is created in this JVM (hence, attempt). It can be used to compare versions of the
   * current entity with any cached copy -it is less brittle than using timestamps.
   */
  val FIELD_ENTITY_VERSION: String = "entityVersion"
  /**
   * Entity `OTHER_INFO` field: Spark version.
   */
  val FIELD_SPARK_VERSION: String = "sparkVersion"
  /**
   * Entity filter field: to search for entities that have started.
   */
  val FILTER_APP_START: String = "startApp"
  /**
   * Value of the `startApp` filter field.
   */
  val FILTER_APP_START_VALUE: String = "SparkListenerApplicationStart"
  /**
   * Entity filter field: to search for entities that have ended.
   */
  val FILTER_APP_END: String = "endApp"
  /**
   * Value of the `endApp`filter field.
   */
  val FILTER_APP_END_VALUE: String = "SparkListenerApplicationEnd"
}
