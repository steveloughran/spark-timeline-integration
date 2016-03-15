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

import java.io.{File, IOException}
import java.net.URL
import java.util.logging.Logger

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileSystem, Path}
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration._
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer
import org.json4s.JValue
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import org.scalatest.concurrent.Eventually

import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient._
import org.apache.spark.status.api.v1.{JobData, StageData}
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.history.{ApplicationHistoryProvider, FsHistoryProvider, HistoryServer}
import org.apache.spark.deploy.history.yarn.{YarnHistoryService, YarnTimelineUtils}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.rest.{HttpOperationResponse, SpnegoUrlConnector}
import org.apache.spark.deploy.history.yarn.server.{TimelineQueryClient, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.testtools.{AbstractYarnHistoryTests, FreePortFinder, HistoryServiceNotListeningToSparkContext, SharedMiniFS, TimelineServiceEnabled}
import org.apache.spark.scheduler.SparkListenerEvent
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
  protected var fileSystem: FileSystem = _

  protected val attempt1SparkId = "spark_id_1"
  protected val attempt2SparkId = "spark_id_2"
  protected val attempt3SparkId = "spark_id_3"

  protected val no_completed_applications = "No completed applications found!"
  protected val no_incomplete_applications = "No incomplete applications found!"

  protected val stdTimeout = timeout(10 seconds)
  protected val stdInterval = interval(100 milliseconds)
  protected val longTimeout = timeout(60 seconds)
  protected val longInterval = interval(1 second)

  // a list of actions to fail with
  protected var failureActions: mutable.MutableList[() => Unit] = mutable.MutableList()

  val REST_BASE = "/api/v1/applications"
  val REST_ALL_APPLICATIONS = s"${REST_BASE}?minDate=1970-01-01"

  def applicationHistoryServer: ApplicationHistoryServer = {
    _applicationHistoryServer
  }

  def timelineClient: TimelineClient = {
    _timelineClient
  }

  def useMiniHDFS: Boolean = false

  /**
   * Setup phase creates all services needed for the tests.
   * 1. A local YARN ATS server.
   * 2. A client of the timeline server.
   * 3. A mini HDFS cluster
   */
  override def setup(): Unit = {
    // abort the tests if the server is offline
    cancelIfOffline()
    startFilesystem()
    super.setup()
    // WADL generator complains needlessly about scala introspection failures.
    val logger =
      Logger.getLogger("com.sun.jersey.server.wadl.generators.WadlGeneratorJAXBGrammarGenerator");
    logger.setLevel(java.util.logging.Level.OFF);
    startTimelineClientAndAHS(sc.hadoopConfiguration)
  }

  /**
   * Set up base configuration for integration tests, including
   * classname bindings in publisher & provider, refresh intervals and a port for the UI.
   *
   * @param sparkConf spark configuration
   * @return the expanded configuration
   */
  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    addHistoryService(sparkConf)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(OPTION_MANUAL_REFRESH_INTERVAL, "1ms")
    sparkConf.set(OPTION_BACKGROUND_REFRESH_INTERVAL, "0s")
    sparkConf.set(OPTION_YARN_LIVENESS_CHECKS, "false")
    sparkConf.set(OPTION_WINDOW_LIMIT, "0")
    sparkConf.setAppName(APP_NAME)
    if (useMiniHDFS) {
      hadoopOpt(sparkConf, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        fileSystem.getUri.toASCIIString)
    }
    sparkConf
  }

  /**
   * Create filesystem.
   *
   * There's a little bit of recursion here in that a configuration may be needed,
   * it can't be the one from the spark context, as that one needs to have the filesystem
   * URI passed in...which can only be done if the port of the FS is known.
   *
   * This is addressed by creating a new, empty Hadoop configuration instance and letting
   * the MiniHDFSCluster start up off that
   */
  protected def startFilesystem(): Unit = {
    if (useMiniHDFS) {
      fileSystem = SharedMiniFS.retrieve(new Configuration())
    } else {
      fileSystem = FileSystem.get(new Configuration())
    }
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
   *
   * @param history history service to stop
   */
  def stopHistoryService(history: YarnHistoryService): Unit = {
    if (history != null && history.serviceState == YarnHistoryService.StartedState) {
      flushHistoryServiceToSuccess()
      history.stop()
      awaitServiceThreadStopped(history, SERVICE_SHUTDOWN_DELAY, false)
      awaitEmptyQueue(historyService, SERVICE_SHUTDOWN_DELAY)
    }
  }

  /**
   * Add an action to execute on failures (if the test runs).
   *
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
      logError("\n===============================\n" +
        "== Executing failure actions ==\n" +
        "================================")
    }
    failureActions.foreach { action =>
      try {
        action()
      } catch {
        case _ : Exception =>
      }
      logError("\n===============================")
    }
  }

  /**
   * Failure action to log history service details at INFO.
   */
  def dumpYarnHistoryService(): Unit = {
    if (historyService != null) {
      logError(s"-- History Service --\n$historyService")
    }
  }

  /**
   * Curryable Failure action to dump provider state.
   *
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
   * Evaluate and log a string at error on any failure.
   * This includes string interpolation.
   *
   * @param msg message function to log
   */
  def failureLog(msg: => String)(): Unit = {
    logError(msg)
  }

  /**
   * Curryable Failure action to log all timeline entities.
   *
   * @param provider the provider bonded to the endpoint
   */
  def dumpTimelineEntities(provider: YarnHistoryProvider)(): Unit = {
    dumpTimelineEntities(provider.getTimelineQueryClient)()
  }

  def dumpTimelineEntities(queryClient: TimelineQueryClient)(): Unit = {
    logError("-- Dumping timeline entities --")
    val entities = queryClient.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    entities.foreach { e =>
      logError(describeEntity(e))
    }
  }

  /**
   * Create a SPNEGO-enabled URL Connector.
   * Picks up the hadoop configuration from `sc`, so the context
   * must be live/non-null
   *
   * @return a URL connector for issuing HTTP requests
   */
  protected def createUrlConnector(): SpnegoUrlConnector = {
    require(sc != null)
    createUrlConnector(sc.hadoopConfiguration)
  }

  /**
   * Create a SPNEGO-enabled URL Connector.
   *
   * @param hadoopConfiguration the configuration to use
   * @return a URL connector for issuing HTTP requests
   */
  def createUrlConnector(hadoopConfiguration: Configuration): SpnegoUrlConnector = {
    SpnegoUrlConnector.newInstance(hadoopConfiguration, new Token)
  }

  /**
   * Create the client and the app server
   *
   * @param conf the hadoop configuration
   */
  protected def startTimelineClientAndAHS(conf: Configuration): Unit = {
    ServiceOperations.stopQuietly(_applicationHistoryServer)
    ServiceOperations.stopQuietly(_timelineClient)
    // turn on ATS 1.5
    if (enableATSv15) {
      enableATS1_5(conf)
    }
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
    val client = new TimelineQueryClient(historyService.timelineWebappAddress,
      historyService.yarnConfiguration,
      createClientConfig())
    client.retryLimit = 0
    client
  }

  /**
   * Put a timeline entity to the timeline client; this is expected
   * to eventually make it to the history server.
   *
   * @param entity entity to put
   * @return the response
   */
  def putTimelineEntity(entity: TimelineEntity): TimelinePutResponse = {
    assertNotNull(_timelineClient, "timelineClient")
    _timelineClient.putEntities(entity)
  }

  /**
   * Marshall and post a spark event to the timeline; return the outcome.
   *
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
   * Flush a history service to success.
   *
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
    assert(0 === history.eventsDropped, s"Dropped events: $history")
  }

  /**
   * Create a history provider instance, following the same process
   * as the history web UI itself: querying the configuration for the
   * provider and falling back to the [[FsHistoryProvider]]. If
   * that falback does take place, however, and assertion is raised.
   *
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
   *
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
   *
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
   *
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
   *
   * @param page web UI
   * @return the body of the response
   */
  protected def jsonAstResource(c: SpnegoUrlConnector, page: URL): JValue = {
    JsonMethods.parse(jsonResource(c, page))
  }

  /**
   * Get a JSON resource. Includes a check that the content type is `application/json`
   *
   * @param page web UI
   * @return the body of the response
   */
  protected def jsonResource(c: SpnegoUrlConnector, page: URL): String = {
    val outcome = c.execHttpOperation("GET", page, null, "")
    logDebug(s"$page => $outcome")
    assert(outcome.contentType.startsWith("application/json"), s"content type of $outcome")
    outcome.responseBody
  }

  /**
   * get a list of app Ids of all apps in a given state. REST API
   *
   * @param c connector
   * @param webUI URL to base web UI
   * @param completed filter by completed or not
   * @return a list of applications
   */
  def listRestAPIApplications(c: SpnegoUrlConnector, webUI: URL, completed: Boolean): Seq[String] = {
    val json = jsonAstResource(c, new URL(webUI, REST_ALL_APPLICATIONS))
    logDebug(s"${JsonMethods.pretty(json)}")
    filterJsonListing(json, completed)
  }

  /**
   * Get the logs as a .zip file.
   *
   * @param c connector
   * @param webUI URL to base web UI
   * @param appId application
   * @param attemptId attempt
   * @return the logs
   */
  def logs(c: SpnegoUrlConnector, webUI: URL, appId: String, attemptId: String)
      : HttpOperationResponse = {
    c.execHttpOperation("GET", new URL(webUI, s"${REST_BASE}/$appId/$attemptId/logs"))
  }

  def listJobsAST(c: SpnegoUrlConnector, webUI: URL, appId: String, attemptId: String): JArray = {
    val json = jsonAstResource(c, new URL(webUI, s"${REST_BASE}/$appId/$attemptId/jobs"))
    logDebug(s"${JsonMethods.pretty(json)}")
    json.asInstanceOf[JArray]
  }

  def listJobs(c: SpnegoUrlConnector, webUI: URL, appId: String, attemptId: String): List[JobData] = {
    jsonMapper.readValue(
      jsonResource(c, new URL(webUI, s"${REST_BASE}/$appId/$attemptId/jobs")),
      classOf[List[JobData]])
  }

  def listJob(c: SpnegoUrlConnector, webUI: URL, appId: String, attemptId: String, jobId: Int)
      : JobData = {
    jsonMapper.readValue(
      jsonResource(c, new URL(webUI, s"${REST_BASE}/$appId/$attemptId/jobs/$jobId")),
      classOf[JobData])
  }

  def listStages(c: SpnegoUrlConnector, webUI: URL, appId: String, attemptId: String): JArray = {
    val json = jsonAstResource(c, new URL(webUI, s"${REST_BASE}/$appId/$attemptId/stages"))
    logDebug(s"${JsonMethods.pretty(json)}")
    json.asInstanceOf[JArray]
  }

  def listStageAttempts(c: SpnegoUrlConnector, webUI: URL, appId: String, attemptId: String, stage: String)
      : JArray = {
    val json = jsonAstResource(c, new URL(webUI, s"${REST_BASE}/$appId/$attemptId/stages/$stage"))
    logDebug(s"${JsonMethods.pretty(json)}")
    json.asInstanceOf[JArray]
  }

  def stage(
      c: SpnegoUrlConnector,
      webUI: URL,
      appId: String,
      attemptId: String,
      stage: Int): List[StageData] = {
    jsonMapper.readValue(
      jsonResource(c,
        new URL(webUI, s"${REST_BASE}/$appId/$attemptId/stages/$stage/")),
      classOf[List[StageData]])
  }

  def stageAttempt(
      c: SpnegoUrlConnector,
      webUI: URL,
      appId: String,
      attemptId: String,
      stage: String,
      stageAttempt: String): StageData = {
    jsonMapper.readValue(
      jsonResource(c,
        new URL(webUI, s"${REST_BASE}/$appId/$attemptId/stages/$stage/$stageAttempt")),
      classOf[StageData])
  }

  def stageMetrics(
      c: SpnegoUrlConnector,
      webUI: URL,
      appId: String,
      attemptId: String,
      stage: String,
      stageAttempt: String): JArray = {
    val json = jsonAstResource(c,
      new URL(webUI, s"${REST_BASE}/$appId/$attemptId/stages/$stage/$stageAttempt/taskSummary"))
    logDebug(s"${JsonMethods.pretty(json)}")
    json.asInstanceOf[JArray]
  }

  def taskList(
      c: SpnegoUrlConnector,
      webUI: URL,
      appId: String,
      attemptId: String,
      stage: String,
      stageAttempt: String): JArray = {
    val json = jsonAstResource(c,
      new URL(webUI, s"${REST_BASE}/$appId/$attemptId/stages/$stage/$stageAttempt/taskList"))
    logDebug(s"${JsonMethods.pretty(json)}")
    json.asInstanceOf[JArray]
  }

  /**
   * Spin awaiting the REST app listing to contain the application
   *
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
      val json = JsonMethods.pretty(jsonAstResource(connector, new URL(url, REST_ALL_APPLICATIONS)))
      fail(s"No app $app in JSON $json")
    }
    awaitHistoryRestUISatisfiesCondition(connector, url,
      (json) => outcomeFromBool(filterJsonListing(json, completed).contains(app)),
    failure, timeout)
  }

  /**
   * Spin awaiting the listing to be a specific size; fail with useful text
   *
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
      val jsonResource = jsonAstResource(connector,
        new URL(url, REST_ALL_APPLICATIONS))
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
   *
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
        condition(jsonAstResource(connector, new URL(url, REST_ALL_APPLICATIONS)))
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
   * Await a the list of entities to match a basic unfiltered listing to match the
   * desired size.
   *
   * @param queryClient client
   * @param expected expected count
   * @return the final list, after the check succeeded
   */
  def awaitEntityListSize(queryClient: TimelineQueryClient, expected: Int): List[TimelineEntity] = {
    def list(): List[TimelineEntity] = {
      listEntities(queryClient)
    }
    awaitCount(expected, TEST_STARTUP_DELAY, () => list().size, s"${list()}")
    list()
  }

  def listEntities(qc: TimelineQueryClient) = {
    qc.listEntities(SPARK_EVENT_ENTITY_TYPE,
      fields = Seq(PRIMARY_FILTERS, OTHER_INFO))
  }

  /**
   * Assert that a list of checks are in the HTML body
   *
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
   *
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
    stopHistoryService(historyService)
    flushHistoryServiceToSuccess()
  }

  /**
   * Create and queue a new [HandleSparkEvent] from the data
   *
   * @param sparkEvent spark event
   */
  def enqueue(sparkEvent: SparkListenerEvent): Unit = {
    eventTime(sparkEvent).getOrElse {
      throw new RuntimeException(s"No time from $sparkEvent")
    }
    assert(historyService.enqueue(sparkEvent))
  }

  /**
   * Post up multiple attempts with the second one a success.
   *
   * This also tells the  `FSTimelineStoreForTesting` that the app is started.
   * At the end of the run, it doesn't stop the spark context, nor the history
   * service, or tag the app as completed...this is to give tests control of
   * their actions here, and access to the spark context.
   */
  def postMultipleAttempts(): Unit = {
    logDebug("posting app start")
    val startTime = 10000
    historyService = startHistoryService(sc, applicationId, Some(attemptId1))
    started(historyService)
    val start1 = appStartEvent(startTime,
      sc.applicationId,
      Utils.getCurrentUserName(),
      Some(attempt1SparkId))
    enqueue(start1)
    enqueue(jobStartEvent(10001, 1))
    enqueue(jobFailureEvent(10002, 1, new scala.RuntimeException("failed")))
    enqueue(appStopEvent(10003))
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
   *
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

  /** flag to indicate whether or not ATS v1.5 should be enabled */
  def enableATSv15: Boolean = true

  /** Name of the plugin; isolated from Java class in case that goes to its own JAR. */
  val PLUGIN_CLASS = "org.apache.spark.deploy.history.yarn.plugin.SparkATSPlugin"

  /**
   * Everything needed to turn on ATS v1.5 timeline server and client.
   *
   * 1. Create the various directories for active and complete apps, plus leveldb, all of
   * the value of `project.build.dir`, or `java.io.tmpdir` if unset.
   * 1. Enable the v1.5 client and ATS server
   * 2. Uses `FSTimelineStoreForTesting` as the store, which doesn't ask YARN about app state.
   * 3. Use `SparkATSPlugin` as the storage plugin.
   * 4. Declares spark events as a summary type.
   * 5. resets the `FSTimelineStoreForTesting` map of appid -> state; all are unknown
   *
   * @param conf configuration to update.
   */
  def enableATS1_5(conf: Configuration): Unit = {
    val projectBuildDir = new File(System.getProperty("project.build.dir",
      System.getProperty("java.io.tmpdir")))
    val integrationDir = new File(projectBuildDir, "integration")
    FileUtils.deleteDirectory(integrationDir)
    integrationDir.mkdirs()
    val leveldbDir = new File(integrationDir, "leveldb")
    leveldbDir.mkdirs()

    conf.setFloat(TIMELINE_SERVICE_VERSION, 1.5f)
    // try to turn off checksums
    conf.setInt(TIMELINE_SERVICE_CLIENT_FD_FLUSH_INTERVAL_SECS, 1)
    conf.setInt(TIMELINE_SERVICE_CLIENT_FD_RETAIN_SECS, 1)

    val fs = fileSystem
    var atsPath: Path  = if (useMiniHDFS) {
      fs.makeQualified(new Path("/history"))
    } else {
      fs.makeQualified(new Path(new File(integrationDir, "ats").toURI))
    }

    logInfo(s"ATS Directory is at $atsPath in filesystem $fs")
    fs.delete(atsPath, true)
    val activeDir = new Path(atsPath, "active")
    fs.mkdirs(activeDir)
    val doneDir = new Path(atsPath, "done")
    fs.mkdirs(doneDir)

    conf.set(TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR, activeDir.toUri.toString)
    conf.set(TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR, doneDir.toUri.toString)
    conf.set(TIMELINE_SERVICE_LEVELDB_PATH, leveldbDir.getAbsolutePath)
    conf.set(TIMELINE_SERVICE_STORE, classOf[FSTimelineStoreForTesting].getName)
    conf.set(TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES, PLUGIN_CLASS)
    conf.setBoolean(TIMELINE_SERVICE_TTL_ENABLE, false)

    conf.setLong(TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS, 1)
    conf.setLong(TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_UNKNOWN_ACTIVE_SECONDS, 10)

    conf.setLong(YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS, 100)
    conf.getLong(YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS, 1000)
    conf.setBoolean(TIMELINE_SERVICE_PREFIX + "entity-file.fs-support-append", false);

    conf.set(TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES,
      "YARN_APPLICATION,YARN_APPLICATION_ATTEMPT,YARN_CONTAINER,"
          + YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    // reset current app map
    FSTimelineStoreForTesting.reset()
  }

  /**
   * Mark the application of a history service as completed.
   *
   * @param history the service
   */
  def completed(history: YarnHistoryService): Unit = {
    completed(history.applicationId)
  }

  /**
   * Mark the application of a history service as started.
   *
   * @param history the service
   */
  def started(history: YarnHistoryService): Unit = {
    started(history.applicationId)
  }

}

