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

## Hadoop YARN Timeline service history provider

As well as the Filesystem History Provider, Spark can integrate with the Hadoop YARN
"Application Timeline Service". This is a service which runs in a YARN cluster, recording
application- and YARN- published events to a database, retrieving them on request.

Spark integrates with the timeline service by
1. Publishing events to the timeline service as applications execute.
1. Listing application histories published to the timeline service.
1. Retrieving the details of specific application histories.

### Configuring the Timeline Service

For details on configuring and starting the timeline service, consult the Hadoop documentation.

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

    <!-- Binding for HTTP endpoint -->
    <property>
      <name>yarn.timeline-service.webapp.address</name>
      <value>atshost.example.org:8188</value>
    </property>

    <property>
      <name>yarn.timeline-service.enabled</name>
      <value>true</value>
    </property>

The root web page of the timeline service can be verified with a web browser,
as an easy check that the service is live.

### Saving Application History to the YARN Timeline Service

To publish to the YARN Timeline Service, Spark applications executed in a YARN cluster
must be configured to instantiate the `YarnHistoryService`. This is done
by setting the spark configuration property `spark.yarn.services`
to `org.apache.spark.deploy.history.yarn.YarnHistoryService`

    spark.yarn.services org.apache.spark.deploy.history.yarn.YarnHistoryService

Notes

1. If the class-name is mis-spelled or cannot be instantiated, an error message will
be logged; the application will still run.
2. YARN history publishing can run alongside the filesystem history listener; both
histories can be viewed by an appropriately configured history service.
3. If the timeline service is disabled, that is `yarn.timeline-service.enabled` is not
`true`, then the history will not be published: the application will still run.
4. Similarly, in a cluster where the timeline service is disabled, the history server
will simply show an empty history, while warning that the history service is disabled.
5. In a secure cluster, the user must have the Kerberos credentials to interact
with the timeline server. Being logged in via `kinit` or a keytab should suffice.
6. If the application is killed it will be listed as incompleted. In an application
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
    Interval in milliseconds between POST retries. Every
    failure adds another delay of this interval before the next retry
    attempt. That is, first 1s, then 2s, 3s, ...
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.batch.size</code></td>
    <td>3</td>
    <td>
    How many events to batch up before submitting them to the timeline service.
    This is a performance optimization.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.post.limit</code></td>
    <td>1000</td>
    <td>
    Limit on number of queued events to posts. When exceeded
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
      incompleted applications are actually still running.
    </td>
  </tr>

</table>


