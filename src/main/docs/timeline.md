<!---
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

## Hadoop YARN Timeline Service Integration

As well as the Filesystem History Provider, Apache Spark can integrate with the Hadoop YARN
"Application Timeline Service". This is a service which runs in a YARN cluster, recording
application- and YARN- published events to a database, retrieving them on request.

Spark integrates with the timeline service by
1. Publishing events to the timeline service as applications execute.
1. Listing application histories published to the timeline service.
1. Retrieving the details of specific application histories.

### Configuring the YARN Timeline Service

For details on configuring and starting the timeline service, consult the Hadoop documentation.

What is critical is to add the `spark-timeline-integration` JAR to the classpath
of the YARN Timeline Server. This contains a Java class which is used to help parse the
events and create summary information from them.

### Configuring `yarn-site.xml`

From the perspective of Spark, the key requirements are
1. The YARN timeline service must be running.
1. Its URL is known, and configured in the `yarn-site.xml` configuration file.
1. The user has an Kerberos credentials required to interact with the service.

The timeline service URL must be declared in the property `yarn.timeline-service.webapp.address`,
or, if HTTPS is the protocol, `yarn.timeline-service.webapp.https.address`

The choice between HTTP and HTTPS is made on the value of `yarn.http.policy`, which can be one of
`http-only` (default), `https_only` or `http_and_https`; HTTP will be used unless the policy
is `https_only`.

Examples:

```xml
<property>
  <name>yarn.timeline-service.enabled</name>
  <value>true</value>
</property>

<!-- Binding for HTTP endpoint -->
<property>
  <name>yarn.timeline-service.webapp.address</name>
  <value>atshost.example.org:8188</value>
</property>

<property>
  <name>yarn.timeline-service.entity-group-fs-store.summary-entity-types</name>
  <value>YARN_APPLICATION,YARN_APPLICATION_ATTEMPT,YARN_CONTAINER,spark_event_v01</value>
</property>

<!-- Classes for other services can be listed, separated by commas -->
<property>
  <name>yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes</name>
  <value>org.apache.spark.deploy.history.yarn.plugin.SparkATSPlugin</value>
</property>
```

The root web page of the timeline service can be verified with a web browser,
as an easy check that the service is live.

### Saving Application History to the YARN Timeline Service

To publish to the YARN Timeline Service, Spark applications executed in a YARN cluster
must be configured to instantiate the `YarnHistoryService`. This is done
by setting the spark configuration property `spark.yarn.services`
to `org.apache.spark.deploy.history.yarn.YarnHistoryService`

    spark.yarn.services org.apache.spark.deploy.history.yarn.YarnHistoryService
    spark.eventLog.enabled true

Notes

1. If `spark.eventLog.enabled` is set to false in the spark context of an application,
logging will be disabled. We recommend this for long-lived streaming applications, as
they generate too much history for the timeline server to play back. Note that
the filesystem log also has issues with very large logs, though less severely.
1. If the class-name is mis-spelled or cannot be instantiated, an error message will
be logged; the application will still run.
1. YARN history publishing can run alongside the filesystem history listener; both
histories can be viewed by an appropriately configured history service.
1. If the timeline service is disabled, that is the YARN configuratin
`yarn.timeline-service.enabled` is not
`true`, then the history will not be published: the application will still run.
1. Similarly, in a cluster where the timeline service is disabled, the history server
will simply show an empty history, while warning that the history service is disabled.
1. In a secure cluster, the user must have the Kerberos credentials to interact
with the timeline server. Being logged in via `kinit` or a keytab should suffice.
1. If the application is killed it will be listed as incompleted. In an application
started as a `--master yarn-client` this happens if the client process is stopped
with a `kill -9` or process failure).
Similarly, an application started with `--master yarn-cluster` will remain incompleted
if killed without warning, if it fails, or it is killed via the `yarn kill` command.


Specific configuration options:

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.domain</code></td>
    <td></td>
    <td>
    If UI permissions are set through `spark.acls.enable` or `spark.ui.acls.enable` being true,
    the optional name of a predefined timeline domain to use . If unset,
    a value is created programmatically.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.post.retry.interval</code></td>
    <td>1s</td>
    <td>
    Interval between POST retries. Every
    failure adds another delay of this interval before the next retry
    attempt. The default sequence is therefore: 1s, then 2s, 3s, ...,
    until <code>spark.hadoop.yarn.timeline.post.retry.max.interval</code>
    is reached.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.post.retry.max.interval</code></td>
    <td>60s</td>
    <td>
    The maximum interval between POST retries.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.batch.size</code></td>
    <td>100</td>
    <td>
    How many events to batch up before submitting them to the timeline service.
    This is a performance optimization.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.post.limit</code></td>
    <td>1000</td>
    <td>
    Limit on number of queued events to post. When exceeded
    new events will be dropped. This is to place a limit on how much
    memory will be consumed if the timeline server goes offline.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.shutdown.waittime</code></td>
    <td>30s</td>
    <td>
    Maximum time in to wait for event posting to complete when the service stops.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.listen</code></td>
    <td>true</td>
    <td>
    This flag exists for testing: if `false` the history publishing
    service will not register for events with the spark context. As
    a result, lifecycle events will not be picked up.
    </td>
  </tr>
</table>

### Viewing Application Histories via the YARN Timeline Service

To retrieve and display history information in the YARN Timeline Service, the Spark history server must
be configured to query the timeline service for the lists of running and completed applications.

Note that the history server does not actually need to be deployed within the Hadoop cluster itself —it
simply needs access to the REST API offered by the timeline service.

To switch to the timeline history, set `spark.history.provider` to
`org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider`:

    spark.history.provider org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider

The Timeline Server bindings in `yarn-site.xml` will also be needed, so that the YARN history
provider can retrieve events from the YARN Timeline Service.

The history provider retrieves data from the timeline service

1. On startup.
1. In the background at an interval set by the option
   `spark.history.yarn.backround.refresh.interval`.
1. When an HTTP request to the web UI or REST API is made and there has not been
any update to the history view in the interval defined by
`spark.history.yarn.manual.refresh.interval`. This triggers an asynchronous update,
so the results are not visible in the HTTP request which triggered the update.

The reason for this design is that in large YARN clusters, frequent polling of
the timeline server to probe for updated applications can place excessive load
on a shared resource.

Together options can offer different policies. Here are some examples, two
at the extremes and some more balanced

#### Background refresh only; updated every minute

      spark.history.yarn.backround.refresh.interval = 300s
      spark.history.yarn.manual.refresh.interval = 0s

The history is updated in the background, once a minute. The smaller
the refresh interval, the higher the load on the timeline server.

#### Manual refresh only; minimum interval one minute

      spark.history.yarn.backround.refresh.interval = 0s
      spark.history.yarn.manual.refresh.interval = 60s

There is no backgroud update; a manual page refresh will trigger an asynchronous refresh.

To get the most current history data, try refreshing the page more than once:
the first to trigger the fetch of the latest data; the second to view the updated history.

This configuration places no load on the YARN Timeline Service when there
is no user of the Spark History Service, at the cost of a slightly less
intuitive UI: because two refreshes are needed to get the updated information,
the state of the system will not be immediately obvious.


#### Manual and background refresh: responsive

      spark.history.yarn.backround.refresh.interval = 60s
      spark.history.yarn.manual.refresh.interval = 20s

Here the background refresh interval is 60s, but a page refresh will trigger
an update if the last refresh was more than 20 seconds ago.

#### Manual and background refresh: low-load

      spark.history.yarn.backround.refresh.interval = 300s
      spark.history.yarn.manual.refresh.interval = 60s

Here a background update takes place every five minutes; a refresh is
also triggered on a page refresh if there has not been one in the last minute.

What makes for the best configuration? It depends on cluster size. The smaller the cluster,
the smaller the background refresh interval can be -and the manual refresh interval then set to zero,
to disable that option entirely.

#### YARN History Provider Configuration Options:

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.history.yarn.window.limit</code></td>
    <td>24h</td>
    <td>
      The earliest time to look for events. The default value is 24 hours.
      This property limits the amount of data queried off the YARN timeline server;
      applications started before this window will not be checked to see if they have completed.
      Set this to 0 for no limits (and increased load on the timeline server).
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.backround.refresh.interval</code></td>
    <td>60s</td>
    <td>
      The interval between background refreshes of the history data.
      A value of 0s means "no background updates: manual refreshes only".
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.manual.refresh.interval</code></td>
    <td>30s</td>
    <td>
      Minimum interval between manual refreshes of the history data; refreshing the
      page before this limit will not trigger an update.
      A value of 0s means "page refreshes do not trigger updates of the application list"
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.event-fetch-limit</code></td>
    <td>1000</td>
    <td>
      Maximum number of application histories to fetch
      from the timeline server in a single GET request.
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.incomplete.refresh.window</code></td>
    <td>1m</td>
    <td>
      Minimum interval for probing for an updated incomplete application.
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.diagnostics</code></td>
    <td>false</td>
    <td>
      A flag to indicate whether low-level diagnostics information should be included in
      status pages. This is for debugging and diagnostics.
    </td>
  </tr>
  </tr>
    <tr>
    <td><code>spark.history.yarn.probe.running.applications</code></td>
    <td>true</td>
    <td>
      Should the history provider query the YARN Resource Manager to verify that
      incompleted applications are actually still running?
    </td>
  </tr>

</table>


## Limitations

The YARN Application Timeline Service v1-1.5 has some limitations on scaleability on availability;
work to address this is ongoing.


1. The 1.0 ATS API (Hadoop 2.6-2.7) is REST-based. If the server is down, then a backlog of events
will build up. The configuration parameter `spark.hadoop.yarn.timeline.post.limit` sets a limit,
after which spark events will be discarded.

1. The 1.5 ATS API saves events to HDFS; data can be written while HDFS is available. The option
`spark.hadoop.yarn.timeline.post.limit` still sets the limit of events to queue, but server
outages, hence queue overflow, is less.

1. There's a limit to how many events can be retrieved from the timeline server for a single
application instance. Tens of thousands of spark events can be stored and retrieved, but
long-lived spark streaming applications will reach these limits.

For this reason, we recommend that logging to the YARN ATS system is *not* used for spark streaming
applications.

## Troubleshooting

Problems related to Spark and ATS integration usually fall into the categories of: scale,
failure handling and security; the classic problems of all large-scale Hadoop applications.

### List of applications is empty

This can happen if

- There are no applications recorded in the YARN timeline service. That can be caused
by the applications not configured to publish to it, or misconfigured so at to be failing to publish.

- The applications are incomplete: check the list of incomplete applications.

- The Spark History Server cannot communicate with the timeline service: check the
logs to see if this is the case.

- The Spark History Server cannot authenticate with the timeline service: check the
logs to see if this is the case.

### List of applications is not updating

The history server only looks for incomplete applications at intermittently, as discussed
above, at regular intervals or after a refresh of the page by (any) user. It may be that
there's a minimum refesh interval on manual refreshes, so as to keep load on the central
timeline server down.

Note also that a transient outage of the YARN Timeline server will stop the application
list being updated.

### Incomplete applications are not updating

An incomplete application will only be updated when:

1. The application's (buffered) event history is eventually saved to the cluster's filesystem or
Posted to the timeline server over HTTP.

1. The Application Timeline Server loads and processes the new events. If events are shared via
the filesystem, this scanning is asynchronous and performed at a configured interval

1. The Spark History Server updates its list of active Spark applications.

1. Either the application has now completed, or a sufficient interval has passed
between the last update (as set in `spark.history.yarn.incomplete.refresh.window` )

What that means is: it takes a while for events to make their way to
the history server, then the UI.

Note also that a transient outage of the YARN Timeline server will stop the application
list being updated.

### Events are generated too fast to be saved

This is logged in the metrics of the Spark Application as dropped events, and in the application
log as dropped events.

1. Increase the number of events posted in a single post via the property
`spark.hadoop.yarn.timeline.batch.size` -such as 500 or 1000.
1. Increase the buffer size of events to queue up for posting.
`spark.hadoop.yarn.timeline.post.limit`.
1. If the YARN timeline server  supports it, use the ATS 1.5 APIs.
These use the cluster filesystem as the means of passing events from the application to the YARN timeline server.

### The application is listed as incomplete/not updating

In some versions of spark, the history server does not update an application once it has been
viewed. This is fixed in Spark 2.0

### The application history doesn't show all its jobs/stages —or they are incomplete

1. When an application history is played back, the configuration option `spark.ui.retainedJobs`
sets a threshold on retained jobs, `spark.ui.retainedStages` on stages. When either of these
thresholds are reached, the Spark UI garbage collects old jobs or stages, as appropriate.

1. It may have been that not all events were recorded during the execution of the application;
this should have been noted in the logs.

### The Spark History Server is not showing any applications

This is usually a sign of the timeline server being unreachable. Check that the YARN timeline
server, is defined in the application configuration and is reachable. The YARN properties are
`yarn.timeline-service.webapp.address`; for HTTPS `yarn.timeline-service.webapp.https.address`.

```
spark.hadoop.yarn.timeline-service.webapp.address
spark.hadoop.yarn.timeline-service.webapp.https.address
spark.hadoop.yarn.timeline-service.enabled true
```

It can also be caused on a secure cluster by the Spark History Server being unable to authenticate
with the Yarn Timeline Service.

To test for the ATS service being reachable, try to retrieve all the spark applications
which it has a record of. These can be listed under the ATS server path
`/ws/v1/timeline/spark_event_v01?fields=PRIMARYFILTERS,OTHERINFO`

If the server is, for example, `http://timelineserver:59587`, the full URL would be:

```
http://timelineserver:59587/ws/v1/timeline/spark_event_v01?fields=PRIMARYFILTERS,OTHERINFO
```

Every application attempt is its own entity in this list; it can be retrieved by name

```
http://timelineserver:59587/ws/v1/timeline/spark_event_v01/appattempt_1111_0000_000000
```

If an application attempt cannot be retrieved, it means that the history server does not have
the data.

### The application is not found

1. It may just not have appeared through the refresh process. Depending upon the refresh
settings, wait a minute or so, possibly refreshing the history page a couple of times.
1. Get the listing of application attempts from the ATS server (as listed above), see if it is there.

### Spark History Server cannot get histories on a secure cluster

(+and the stack traces show security messages in there)

This is inevitably some kind of Kerberos configuration problem.

Consult the [list of common error messages](https://steveloughran.gitbooks.io/kerberos_and_hadoop/content/sections/errors.html).

### Spark History Server stops being able to read histories after 24/48/72h

The Kerberos tokens of whoever started the service have expired; the keytab refresh isn't helping.

## Adding extra diagnostics to the history server.

To aid troubleshooting the Spark History server, set the diagnostics option:

```
spark.history.yarn.diagnostics
```

This adds extra information in the Web UI, including the URL needed to directly
query the YARN history server


## Disabling History logging on a specific Spark Application

There are multiple ways to do this:

Set the list of of Yarn service to load to ""/delete the line entirely

    spark.yarn.services

Tell the spark application that the YARN timeline service is not running

    spark.hadoop.yarn.timeline-service.enabled false

This still loads the history service plugin into the Spark Application —but it does not
attempt to publish any history events to YARN.

