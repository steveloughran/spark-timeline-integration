
# SPARK-1537: Add integration with Yarn's Application Timeline Server

## A library for integrating Apache Spark with the Apache Hadoop YARN Timeline Server and 


This the code behind [SPARK-1537](https://issues.apache.org/jira/browse/SPARK-1537), *Add integration with Yarn's Application Timeline Server*, pulled out into its own repo
while the PR awaits review and incorporation.

While standalone, to build it, the repo needs to go in under spark/; the root POM and that of the
spark-assembly patched to include it.

License: ASF, obviously


Apache Hadoop and Apache Spark are registered trademarks of the Apache Software Foundation.