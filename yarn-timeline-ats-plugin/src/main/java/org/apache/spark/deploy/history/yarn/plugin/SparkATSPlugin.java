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

package org.apache.spark.deploy.history.yarn.plugin;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineEntityGroupPlugin;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

/**
 * This class is designed to be loaded in the YARN application timeline server.
 * <i>Important:</i> this must not include any dependencies which aren't already on the ATS
 * classpath. No references to Spark classes, use of Scala etc.
 * This is why it is in Java
 */
public class SparkATSPlugin extends TimelineEntityGroupPlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkATSPlugin.class);


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


  public SparkATSPlugin() {
    LOG.info("SparkATSPlugin");
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityType,
      NameValuePair filter, Collection<NameValuePair> secondaryFilters) {
    LOG.debug("getTimelineEntityGroupId({},[{}]", entityType, filter);

    Set<TimelineEntityGroupId> result = null;
    try {
      if (entityType.equals(SPARK_EVENT_ENTITY_TYPE) && filter != null) {
        String value = filter.getValue().toString();
        switch (filter.getName()) {
          case FIELD_APPLICATION_ID:
            result = toEntityGroupId(value);
            break;
          case FIELD_ATTEMPT_ID:
            result = toGroupId(entityToApplicationId(value));
            break;
          default:
            // no-op

        }
      }
    } catch (Exception e) {
      LOG.error("Failed to process entity type {} and primary filter {}",
          entityType, filter, e);
      throw new IllegalArgumentException(e);
    }
    return result;
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityId,
      String entityType) {
    LOG.debug("getTimelineEntityGroupId({}}, {}})", entityId, entityType );

    if (entityType.equals(SPARK_EVENT_ENTITY_TYPE)) {
      return toGroupId(entityToApplicationId(entityId));
    } else {
      return null;
    }
  }

  /**
   * Converts an entity ID to an application ID. This works with an appID or attempt ID
   * string: an application Attempt ID is tried first, and if that fails, its tried as an
   * application
   * @param entityId string of the entity
   * @return an application ID extracted from the entity
   * @throws IllegalArgumentException if it could not be converted
   */
  private ApplicationId entityToApplicationId(String entityId) {
    try {
      return ConverterUtils.toApplicationAttemptId(entityId).getApplicationId();
    } catch (IllegalArgumentException e) {
      return ConverterUtils.toApplicationId(entityId);
    }
  }

  @Override
  public Set<TimelineEntityGroupId> getTimelineEntityGroupId(String entityType,
      SortedSet<String> entityIds, Set<String> eventTypes) {
    LOG.debug("getTimelineEntityGroupId($entityType)");

    return null;
  }

  private Set<TimelineEntityGroupId> toEntityGroupId(String applicationId) {
    return toGroupId(ConverterUtils.toApplicationId(applicationId));
  }

  private Set<TimelineEntityGroupId> toGroupId(ApplicationId applicationId) {
    TimelineEntityGroupId groupId = TimelineEntityGroupId.newInstance(
        applicationId,
        SPARK_EVENT_GROUP_TYPE);
    LOG.debug("mapped {} to {}, ", applicationId, groupId);
    Set<TimelineEntityGroupId> result = new HashSet<>();
    result.add(groupId);
    return result;
  }
}
