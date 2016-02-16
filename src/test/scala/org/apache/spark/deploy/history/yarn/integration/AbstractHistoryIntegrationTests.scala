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

import java.io.IOException
import java.net.URL
import java.util.logging.Logger

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.history.{ApplicationHistoryProvider, FsHistoryProvider, HistoryServer}
import org.apache.spark.deploy.history.yarn.{YarnHistoryService, YarnTimelineUtils}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.rest.SpnegoUrlConnector
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.testtools.{AbstractYarnHistoryTests, FreePortFinder, HistoryServiceNotListeningToSparkContext, TimelineServiceEnabled}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.scheduler.cluster.{SchedulerExtensionServices, StubApplicationAttemptId}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils

/**
 * Integration tests with history services setup and torn down
 */
abstract class AbstractHistoryIntegrationTests
    extends AbstractYarnHistoryTests
    with FreePortFinder
    with HistoryServiceNotListeningToSparkContext
    with TimelineServiceEnabled
    with IntegrationTestUtils
    with Eventually {

  protected val PackagePath = "org/apache/spark/deploy/history/yarn/integration/"
  protected var _applicationHistoryServer: ApplicationHistoryServer = _
  protected var _timelineClient: TimelineClient = _
  protected var historyService: YarnHistoryService = _
  protected var sparkHistoryServer: HistoryServer = _

  protected val attempt1SparkId = "spark_id_1"
  protected val attempt2SparkId = "spark_id_2"
  protected val attempt3SparkId = "spark_id_3"

  protected val no_completed_applications = "No completed applications found!"
  protected val no_incomplete_applications = "No incomplete applications found!"

  // a list of actions to fail with
  protected var failureActions: mutable.MutableList[() => Unit] = mutable.MutableList()

  val REST_APPLICATIONS = "/api/v1/applications/?minDate=1970-01-01"

  def applicationHistoryServer: ApplicationHistoryServer = {
    _applicationHistoryServer
  }

  def timelineClient: TimelineClient = {
    _timelineClient
  }

  /*
   * Setup phase creates a local ATS server and a client of it
   */

  override def setup(): Unit = {
    // abort the tests if the server is offline
    cancelIfOffline()
    super.setup()
    // WADL generator complains needlessly about scala introspection failures.
    val logger =
      Logger.getLogger("com.sun.jersey.server.wadl.generators.WadlGeneratorJAXBGrammarGenerator");
    logger.setLevel(java.util.logging.Level.OFF);
    startTimelineClientAndAHS(sc.hadoopConfiguration)
  }

  /**
   * Set up base configuratin for integration tests, including
   * classname bindings in publisher & provider, refresh intervals and a port for the UI
   * @param sparkConf spark configuration
   * @return the expanded configuration
   */
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(SchedulerExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(OPTION_MANUAL_REFRESH_INTERVAL, "1ms")
    sparkConf.set(OPTION_BACKGROUND_REFRESH_INTERVAL, "0s")
    sparkConf.set(OPTION_YARN_LIVENESS_CHECKS, "false")
    sparkConf.set(OPTION_WINDOW_LIMIT, "0")
    sparkConf.setAppName(APP_NAME)
  }

  /**
   * Stop all services, including, if set, anything in
   * <code>historyService</code>
   */
  override def afterEach(): Unit = {
    describe("Teardown of history server, timeline client and history service")
    stopHistoryService(historyService)
    historyService = null
    ServiceOperations.stopQuietly(_applicationHistoryServer)
    _applicationHistoryServer = null
    ServiceOperations.stopQuietly(_timelineClient)
    _timelineClient = null
    super.afterEach()
  }

  /**
   * Stop a history service. This includes flushing its queue,
   * blocking until that queue has been flushed and closed, then
   * stopping the YARN service.
   * @param hservice history service to stop
   */
  def stopHistoryService(hservice: YarnHistoryService): Unit = {
    if (hservice != null && hservice.serviceState == YarnHistoryService.StartedState) {
      flushHistoryServiceToSuccess()
      hservice.stop()
      awaitServiceThreadStopped(hservice, TEST_STARTUP_DELAY, false)
    }
  }

  /**
   * Add an action to execute on failures (if the test runs it
   * @param action action to execute
   */
  def addFailureAction(action: () => Unit) : Unit = {
    failureActions += action
  }

  /**
   * Execute all the failure actions in order.
   */
  def executeFailureActions(): Unit = {
    if (failureActions.nonEmpty) {
      logError("== Executing failure actions ==")
    }
    failureActions.foreach{ action =>
      try {
        action()
      } catch {
        case _ : Exception =>
      }
    }
  }

  /**
   * Failure action to log history service details at INFO
   */
  def dumpYarnHistoryService(): Unit = {
    if (historyService != null) {
      logError(s"-- History Service --\n$historyService")
    }
  }

  /**
   * Curryable Failure action to dump provider state
   * @param provider the provider
   */
  def dumpProviderState(provider: YarnHistoryProvider)(): Unit = {
    logError(s"-- Provider --\n$provider")
    val results = provider.getApplications
    results.applications.foreach{
    app =>
      logError(s" $app")
    }
    results.failureCause.foreach{ e =>
      logError("Failed", e)
    }
  }

  /**
   * Curryable Failure action to log all timeline entities
   * @param provider the provider bonded to the endpoint
   */
  def dumpTimelineEntities(provider: YarnHistoryProvider)(): Unit = {
    logError("-- Dumping timeline entities --")
    val entities = provider.getTimelineQueryClient
        .listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    entities.foreach{ e =>
      logError(describeEntity(e))
    }
  }

  /**
   * Create a SPNEGO-enabled URL Connector.
   * Picks up the hadoop configuration from `sc`, so the context
   * must be live/non-null
   * @return a URL connector for issuing HTTP requests
   */
  protected def createUrlConnector(): SpnegoUrlConnector = {
    require(sc != null)
    createUrlConnector(sc.hadoopConfiguration)
  }

  /**
   * Create a SPNEGO-enabled URL Connector.
   * @param hadoopConfiguration the configuration to use
   * @return a URL connector for issuing HTTP requests
   */
  def createUrlConnector(hadoopConfiguration: Configuration): SpnegoUrlConnector = {
    SpnegoUrlConnector.newInstance(hadoopConfiguration, new Token)
  }

  /**
   * Create the client and the app server
   * @param conf the hadoop configuration
   */
  protected def startTimelineClientAndAHS(conf: Configuration): Unit = {
    ServiceOperations.stopQuietly(_applicationHistoryServer)
    ServiceOperations.stopQuietly(_timelineClient)
    _timelineClient = TimelineClient.createTimelineClient()
    _timelineClient.init(conf)
    _timelineClient.start()
    _applicationHistoryServer = new ApplicationHistoryServer()
    _applicationHistoryServer.init(_timelineClient.getConfig)
    _applicationHistoryServer.start()
    // Wait for AHS to come up
    val endpoint = YarnTimelineUtils.timelineWebappUri(conf, "")
    awaitURL(endpoint.toURL, TEST_STARTUP_DELAY)
  }

  protected def createTimelineQueryClient(): TimelineQueryClient = {
    new TimelineQueryClient(historyService.timelineWebappAddress,
      sc.hadoopConfiguration, createClientConfig())
  }

  /**
   * Put a timeline entity to the timeline client; this is expected
   * to eventually make it to the history server
   * @param entity entity to put
   * @return the response
   */
  def putTimelineEntity(entity: TimelineEntity): TimelinePutResponse = {
    assertNotNull(_timelineClient, "timelineClient")
    _timelineClient.putEntities(entity)
  }

  /**
   * Marshall and post a spark event to the timeline; return the outcome
   * @param sparkEvt event
   * @param time event time
   * @return a triple of the wrapped event, marshalled entity and the response
   */
  protected def postEvent(sparkEvt: SparkListenerEvent, time: Long):
      (TimelineEvent, TimelineEntity, TimelinePutResponse) = {
    val event = toTimelineEvent(sparkEvt, time).get
    val entity = newEntity(time)
    entity.addEvent(event)
    val response = putTimelineEntity(entity)
    val description = describePutResponse(response)
    logInfo(s"response: $description")
    assert(response.getErrors.isEmpty, s"errors in response: $description")
    (event, entity, response)
  }

  /**
   * flush the history service of its queue, await it to complete,
   * then assert that there were no failures
   */
  protected def flushHistoryServiceToSuccess(): Unit = {
    flushHistoryServiceToSuccess(historyService)
  }

  /**
   * Flush a history service to success
   * @param history service to flush
   * @param delay time to wait for an empty queue
   */
  def flushHistoryServiceToSuccess(
      history: YarnHistoryService,
      delay: Int = TEST_STARTUP_DELAY): Unit = {
    assertNotNull(history, "null history queue")
    historyService.asyncFlush()
    awaitEmptyQueue(history, delay)
    assert(0 === history.postFailures, s"Post failure count: $history")
  }

  /**
   * Create a history provider instance, following the same process
   * as the history web UI itself: querying the configuration for the
   * provider and falling back to the [[FsHistoryProvider]]. If
   * that falback does take place, however, and assertion is raised.
   * @param conf configuration
   * @return the instance
   */
  protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    val providerName = conf.getOption("spark.history.provider")
      .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = Utils.classForName(providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)
      .asInstanceOf[ApplicationHistoryProvider]
    assert(provider.isInstanceOf[YarnHistoryProvider],
      s"Instantiated $providerName to get $provider")

    provider.asInstanceOf[YarnHistoryProvider]
  }

  /**
   * Crete a history server and maching provider, execute the
   * probe against it. After the probe completes, the history server
   * is stopped.
   * @param probe probe to run
   */
  def webUITest(name: String, probe: (URL, YarnHistoryProvider) => Unit): Unit = {
    val (_, server, webUI, provider) = createHistoryServer(findPort())
    try {
      sparkHistoryServer = server
      server.bind()
      describe(name)
      probe(webUI, provider)
    } catch {
      case ex: Exception =>
        executeFailureActions()
        throw ex
    } finally {
      describe("stopping history service")
      Utils.tryLogNonFatalError {
        server.stop()
      }
      sparkHistoryServer = null
    }
  }

  /**
   * Probe the empty web UI for not having any completed apps; expect
   * a text/html response with specific text and history provider configuration
   * elements.
   * @param webUI web UI
   * @param provider provider
   */
  def probeEmptyWebUI(webUI: URL, provider: YarnHistoryProvider): String = {
    val body: String = getHtmlPage(webUI,
       "<title>History Server</title>"
        :: YarnHistoryProvider.KEY_PROVIDER_NAME
        :: YarnHistoryProvider.PROVIDER_DESCRIPTION
        :: Nil)
    logInfo(s"$body")
    body
  }

  /**
   * Get an HTML page. Includes a check that the content type is `text/html`
   * @param page web UI
   * @param checks list of strings to assert existing in the response
   * @return the body of the response
   */
  protected def getHtmlPage(page: URL, checks: List[String]): String = {
    val outcome = createUrlConnector().execHttpOperation("GET", page, null, "")
    logDebug(s"$page => $outcome")
    assert(outcome.contentType.startsWith("text/html"), s"content type of $outcome")
    val body = outcome.responseBody
    assertStringsInBody(body, checks)
    body
  }

  /**
   * Get a JSON resource. Includes a check that the content type is `application/json`
   * @param page web UI
   * @return the body of the response
   */
  protected def getJsonResource(c: SpnegoUrlConnector, page: URL): JValue = {
    val outcome = c.execHttpOperation("GET", page, null, "")
    logDebug(s"$page => $outcome")
    assert(outcome.contentType.startsWith("application/json"), s"content type of $outcome")
    JsonMethods.parse(outcome.responseBody)
  }

  // get a list of app Ids of all apps in a given state. REST API
  def listRestAPIApplications(c: SpnegoUrlConnector, webUI: URL, completed: Boolean): Seq[String] = {
    val json = getJsonResource(c, new URL(webUI, REST_APPLICATIONS))
    logDebug(s"${JsonMethods.pretty(json)}")
    filterJsonListing(json, completed)
  }

  /**
   * Spin awaiting the REST app listing to contain the application
   * @param connector connector to use
   * @param url web UI
   * @param app text which must be present
   * @param completed filter completed or incomplete?
   * @param timeout timeout in millis
   */
  def awaitHistoryRestUIContainsApp(
      connector: SpnegoUrlConnector,
      url: URL,
      app: String,
      completed: Boolean,
      timeout: Long): Unit = {
    def failure(outcome: Outcome, iterations: Int, timeout: Boolean): Unit = {
      val json = JsonMethods.pretty(getJsonResource(connector, new URL(url, REST_APPLICATIONS)))
      fail(s"No app $app in JSON $json")
    }
    awaitHistoryRestUISatisfiesCondition(connector, url,
      (json) => outcomeFromBool(filterJsonListing(json, completed).contains(app)),
    failure, timeout)
  }

  /**
   * Spin awaiting the listing to be a specific size; fail with useful text
   * @param connector connector to use
   * @param url URL to probe
   * @param size size of list
   * @param completed filter completed or incomplete?
   * @param timeout timeout in millis
   */
  def awaitHistoryRestUIListSize(
      connector: SpnegoUrlConnector,
      url: URL,
      size: Int,
      completed: Boolean,
      timeout: Long): Unit = {
    def failure(outcome: Outcome, iterations: Int, timeout: Boolean): Unit = {
      val jsonResource = getJsonResource(connector,
        new URL(url, REST_APPLICATIONS))
      val listing = filterJsonListing(jsonResource, completed)
      val prettyJson = JsonMethods.pretty(jsonResource)
      fail(s"Expected list size $size got ${listing.size} in $listing from JSON $prettyJson")
    }
    awaitHistoryRestUISatisfiesCondition(connector, url,
      (json) => outcomeFromBool(filterJsonListing(json, completed).size == size),
    failure, timeout)
  }

  /**
   * Await the Rest UI to satisfy some condition
   * @param connector connector to use
   * @param url web UI
   * @param condition condition to be met
   * @param failure failure operation
   * @param timeout timeout in millis
   */
  def awaitHistoryRestUISatisfiesCondition(
      connector: SpnegoUrlConnector,
      url: URL,
      condition: JValue => Outcome ,
      failure: (Outcome, Int, Boolean) => Unit,
      timeout: Long): Unit = {
    def probe(): Outcome = {
      try {
        condition(getJsonResource(connector, new URL(url, REST_APPLICATIONS)))
      } catch {
        case ioe: IOException =>
          Retry()
        case ex: Exception =>
          throw ex
      }
    }
    spinForState(s"Awaiting a response from URL $url",
      interval = 50, timeout = timeout, probe = probe, failure = failure)
  }

  /**
   * Assert that a list of checks are in the HTML body
   * @param body body of HTML (or other string)
   * @param checks list of strings to assert are present
   */
  def assertStringsInBody(body: String, checks: List[String]): Unit = {
    var missing: List[String] = Nil
    var text = "[ "
    checks foreach { check =>
      if (!body.contains(check)) {
        missing = check :: missing
        text = text +"\"" + check +"\" "
      }
    }
    text = text + "]"
    if (missing.nonEmpty) {
      fail(s"Did not find $text in\n$body")
    }
  }

  /**
   * Create a [[HistoryServer]] instance with a coupled history provider.
   * @param defaultPort a port to use if the property `spark.history.ui.port` isn't
   *          set in the spark context. (default: 18080)
   * @return (port, server, web UI URL, history provider)
   */
  protected def createHistoryServer(defaultPort: Int = 18080):
     (Int, HistoryServer, URL, YarnHistoryProvider) = {
    val conf = sc.conf
    val securityManager = new SecurityManager(conf)
    val args: List[String] = Nil
    val port = conf.getInt(SPARK_HISTORY_UI_PORT, defaultPort)
    val provider = createHistoryProvider(sc.getConf)
    val server = new HistoryServer(conf, provider, securityManager, port)
    val webUI = new URL("http", "localhost", port, "/")
    (port, server, webUI, provider)
  }

  /**
   * closing context generates an application stop
   */
  def stopContextAndFlushHistoryService(): Unit = {
    describe("stopping context")
    resetSparkContext()
    flushHistoryServiceToSuccess()
  }

  /**
   * Create and queue a new [HandleSparkEvent] from the data
   * @param sparkEvent spark event
   */
  def enqueue(sparkEvent: SparkListenerEvent): Unit = {
    eventTime(sparkEvent).getOrElse {
      throw new RuntimeException(s"No time from $sparkEvent")
    }
    assert(historyService.enqueue(sparkEvent))
  }

  /**
   * Post up multiple attempts with the second one a success
   */
  def postMultipleAttempts(): Unit = {
    logDebug("posting app start")
    val startTime = 10000
    historyService = startHistoryService(sc, applicationId, Some(attemptId1))
    val start1 = appStartEvent(startTime,
      sc.applicationId,
      Utils.getCurrentUserName(),
      Some(attempt1SparkId))
    enqueue(start1)
    enqueue(jobStartEvent(10001, 1))
    enqueue(jobFailureEvent(10002, 1, new scala.RuntimeException("failed")))
    enqueue(appStopEvent(10003))
    flushHistoryServiceToSuccess()
    stopHistoryService(historyService)

    // second attempt
    val start2Time = 20000
    historyService = startHistoryService(sc, applicationId, Some(attemptId2))
    val start2 = appStartEvent(start2Time,
      sc.applicationId,
      Utils.getCurrentUserName(),
      Some(attempt2SparkId))
    enqueue(start2)

    enqueue(jobStartEvent(20000, 1))
    enqueue(jobSuccessEvent(20002, 1))
    enqueue(appStopEvent(20003))

    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)
  }

  /**
   * Get the provider UI with an assertion failure if none came back
   * @param provider provider
   * @param appId app ID
   * @param attemptId optional attempt ID
   * @return the provider UI retrieved
   */
  def getAppUI(provider: YarnHistoryProvider,
      appId: String,
      attemptId: Option[String]): SparkUI = {
    val loadedAppUI = provider.getAppUI(appId, attemptId)
    assertSome(loadedAppUI, s"Failed to retrieve App UI under ID $appId attempt $attemptId from $provider")
    loadedAppUI.get.ui
  }
}
