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

import java.io.{ByteArrayInputStream, FileNotFoundException, IOException}
import java.net.URI

import scala.Predef.assert

import com.sun.jersey.api.client.{ClientHandlerException, ClientResponse, UniformInterfaceException}
import org.json4s.jackson.JsonMethods

import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, UnauthorizedRequestException}
import org.apache.spark.deploy.history.yarn.testtools.AbstractYarnHistoryTests
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

class ParseRestApiSuite extends AbstractYarnHistoryTests {
  protected val PackagePath = "org/apache/spark/deploy/history/yarn/integration/"

  test("incomplete") {
    val r = filterJsonListing(loadToJson(PackagePath + "rest-incomplete.json"), false)
    assert (r.size === 1, s"Wrong size of $r")
  }
  test("complete") {
    val r = filterJsonListing(loadToJson(PackagePath + "rest-incomplete.json"), true)
    assert (r.size === 0, s"Wrong size of $r")
  }
}
