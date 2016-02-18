/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history.yarn;

/**
 * Constant fields for marking up the entities.
 *
 * <i>Important:</i> these must be kept in sync with the scala definitions in
 * {@code org.apache.spark.deploy.history.yarn.YarnHistoryService}.
 *
 * There are no cross references so that the java plugin may be isolated in future
 */
public interface EntityConstants {


  /**
   * Name of the entity type used to declare spark Applications.
   */
  public static final String SPARK_EVENT_ENTITY_TYPE = "spark_event_v01";

  /**
   * Name of the entity type used to declare spark Applications.
   */
  public static final String SPARK_EVENT_GROUP_TYPE = "spark_event_group_v01";

  /**
   * Domain ID.
   */
  public static final String DOMAIN_ID_PREFIX = "Spark_ATS_";

  /**
   * Primary key used for events
   */
  public static final String PRIMARY_KEY = "spark_application_entity";

  /**
   *  Entity `OTHER_INFO` field: start time
   */
  public static final String FIELD_START_TIME = "startTime";

  /**
   * Entity `OTHER_INFO` field: last updated time.
   */
  public static final String FIELD_LAST_UPDATED = "lastUpdated";

  /**
   * Entity `OTHER_INFO` field: end time. Not present if the app is running.
   */
  public static final String FIELD_END_TIME = "endTime";

  /**
   * Entity `OTHER_INFO` field: application name from context.
   */
  public static final String FIELD_APP_NAME = "appName";

  /**
   * Entity `OTHER_INFO` field: user.
   */
  public static final String FIELD_APP_USER = "appUser";

  /**
   * Entity `OTHER_INFO` field: YARN application ID.
   */
  public static final String FIELD_APPLICATION_ID = "applicationId";

  /**
   * Entity `OTHER_INFO` field: attempt ID from spark start event.
   */
  public static final String FIELD_ATTEMPT_ID = "attemptId";

  /**
   * Entity `OTHER_INFO` field: a counter which is incremented whenever a new timeline entity
   * is created in this JVM (hence, attempt). It can be used to compare versions of the
   * current entity with any cached copy -it is less brittle than using timestamps.
   */
  public static final String FIELD_ENTITY_VERSION = "entityVersion";

  /**
   * Entity `OTHER_INFO` field: Spark version.
   */
  public static final String FIELD_SPARK_VERSION = "sparkVersion";

  /**
   * Entity filter field: to search for entities that have started.
   */
  public static final String FILTER_APP_START = "startApp";

  /**
   * Value of the `startApp` filter field.
   */
  public static final String FILTER_APP_START_VALUE = "SparkListenerApplicationStart";

  /**
   * Entity filter field: to search for entities that have ended.
   */
  public static final String FILTER_APP_END = "endApp";

  /**
   * Value of the `endApp`filter field.
   */
  public static final String FILTER_APP_END_VALUE = "SparkListenerApplicationEnd";


}
