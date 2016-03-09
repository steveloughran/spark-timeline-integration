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

import org.json4s.JValue
import org.json4s.jackson.JsonMethods

import org.apache.spark.deploy.history.yarn.testtools.AbstractYarnHistoryTests
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.status.api.v1.JobData

/**
 * Verifies that the code to parse the Spark History REST API works
 */
class ParseRestApiSuite extends AbstractYarnHistoryTests {
  val PackagePath = "org/apache/spark/deploy/history/yarn/integration/"
  val IncompleteResponse = loadJsonAST(PackagePath + "rest-incomplete.json")
  val CompleteResponse = loadJsonAST(PackagePath + "rest-2-complete.json")
  val MixedResponse = loadJsonAST(PackagePath + "rest-3-mixed.json")

  def expectListingSize(response: JValue, completed: Boolean, size: Int): Unit = {
    val r = filterJsonListing(response, completed)
    assert(r.size === size, s"Wrong size of $r from\n${JsonMethods.pretty(response)}")
  }

  test("one incomplete in incomplete results") {
    expectListingSize(IncompleteResponse, false, 1)
  }

  test("no complete in incomplete") {
    expectListingSize(IncompleteResponse, true, 0)
  }

  test("no incomplete in complete") {
    expectListingSize(CompleteResponse, false, 0)
  }

  test("one complete in complete") {
    expectListingSize(CompleteResponse, true, 1)
  }

  test("one complete in mixed") {
    expectListingSize(MixedResponse, true, 1)
  }

  test("one incomplete in mixed") {
    expectListingSize(MixedResponse, false, 1)
  }

  val JobJSON =
    """
      |{
      |  "jobId" : 0,
      |  "name" : "count at WebsiteIntegrationSuite.scala:60",
      |  "submissionTime" : "2016-03-09T14:13:04.411GMT",
      |  "completionTime" : "2016-03-09T14:13:04.655GMT",
      |  "stageIds" : [ 0 ],
      |  "status" : "SUCCEEDED",
      |  "numTasks" : 1,
      |  "numActiveTasks" : 0,
      |  "numCompletedTasks" : 1,
      |  "numSkippedTasks" : 1,
      |  "numFailedTasks" : 0,
      |  "numActiveStages" : 0,
      |  "numCompletedStages" : 1,
      |  "numSkippedStages" : 0,
      |  "numFailedStages" : 0
      |}
    """.stripMargin

  test("JobDeser") {
    jsonMapper.readValue(JobJSON, classOf[JobData])
  }
}
