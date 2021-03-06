<!--
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

Apache Accumulo Functional Tests
================================

These scripts run a series of tests against a small local accumulo instance.
To run these scripts, you must have Hadoop and Zookeeper installed and running.
You will need a functioning C compiler to build a shared library needed for
one of the tests.  The test suite is known to run on Linux RedHat Enterprise
version 5, and Mac OS X 10.5.

How to Run
----------

The tests are shown as being run from the `ACCUMULO_HOME` directory, but they
should run from any directory. Make sure to create "logs" and "walogs"
directories in `ACCUMULO_HOME`.  Also, ensure that `accumulo-env.sh` specifies its
`ACCUMULO_LOG_DIR` in the following way:

> `$ test -z "$ACCUMULO_LOG_DIR" && export ACCUMULO_LOG_DIR=$ACCUMULO_HOME/logs`

To list all the test names:

> `$ ./test/system/auto/run.py -l`

You can run the suite like this:

> `$ ./test/system/auto/run.py`

You can select tests using a case-insensitive regular expression:

> `$ ./test/system/auto/run.py -t simple`  
> `$ ./test/system/auto/run.py -t SunnyDay`

To run tests repeatedly:

> `$ ./test/system/auto/run.py -r 3`

If you are attempting to debug what is causing a test to fail, you can run the
tests in "verbose" mode:

> `$ python test/system/auto/run.py -t SunnyDay -v 10`

If a test is failing, and you would like to examine logs from the run, you can
run the test in "dirty" mode which will keep the test from cleaning up all the
logs at the end of the run:

> `$ ./test/system/auto/run.py -t some.failing.test -d`

If the test suite hangs, and you would like to re-run the tests starting with
the last test that failed:

> `$ ./test/system/auto/run.py -s start.over.test`

If tests tend to time out (on slower hardware, for example), you can scale up
the timeout values by a multiplier. This example triples timeouts:

> `$ ./test/system/auto/run.py -f 3`

Test results are normally printed to the console, but you can send them to XML
files compatible with Jenkins:

> `$ ./test/system/auto/run.py -x`

Running under MapReduce
-----------------------

The full test suite can take nearly an hour.  If you have a larger Hadoop
cluster at your disposal, you can run the tests as a MapReduce job:

> `$ python test/system/auto/run.py -l > tests  
$ hadoop fs -put tests /user/hadoop/tests  
$ ./bin/accumulo org.apache.accumulo.test.functional.RunTests --tests \  
    /user/hadoop/tests --output /user/hadoop/results`

The example above runs every test. You can trim the tests file to include
only the tests you wish to run.

You may specify a 'timeout factor' via an optional integer argument:

> `$ ./bin/tool.sh lib/accumulo-test.jar org.apache.accumulo.test.functional.RunTests --tests \
/user/hadoop/tests --output /user/hadoop/results --timeoutFactor timeout_factor`

Where `timeout_factor` indicates how much we should scale up timeouts. It will
be used to set both `mapred.task.timeout` and the "-f" flag used by `run.py`. If
not given, `timeout_factor` defaults to 1, which corresponds to a
`mapred.task.timeout` of 480 seconds.

In some clusters, the user under which MR jobs run is different from the user
under which Accumulo is installed, and this can cause failures running the
tests. Various configuration and permission changes can be made to help the
tests run, including the following:

* Opening up directory and file permissions on each cluster node so that the MR
  user has the same read/write privileges as the Accumulo user. Adding the MR
  user to a shared group is one easy way to accomplish this. Access is required
  to the Accumulo installation, log, write-ahead log, and configuration
  directories.
* Creating a user directory in HDFS, named after and owned by the MR user,
  e.g., `/user/mruser`.
* Setting the `ZOOKEEPER_HOME` and `HADOOP_CONF_DIR` environment variables for the
  MR user. These can be set using the `mapred.child.env` property in
  `mapred-site.xml`, e.g.:

  `<property>  
    <name>mapred.child.env</name>  
    <value>ZOOKEEPER_HOME=/path/to/zookeeper,HADOOP_CONF_DIR=/path/to/hadoop/conf</value>  
  </property>`

Each functional test is run by a mapper, and so you can check the mapper logs
to see any error messages tests produce.
