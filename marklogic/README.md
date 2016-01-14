<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# MarkLogic Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a MarkLogic Server cluster. It uses the official MarkLogic Java SDK.

## Quickstart

### 1. Start MarkLogic Server
You need to start a single node or a cluster to point the client at. 

### 2. Set up YCSB
You need to clone the repository and compile everything.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn clean package
```

### 3. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb load marklogic -s -P workloads/workloada
```

Then, you can run the workload:

```
bin/ycsb run marklogic -s -P workloads/workloada
```

Please see the general instructions in the `doc` folder if you are not sure how it all works. You can apply a property (as seen in the next section) like this:

```
bin/ycsb run marklogic -s -P workloads/workloada -p marklogic.json=false -p marklogic.batchsize=100
```

## Configuration Options
Since no setup is the same and the goal of YCSB is to deliver realistic benchmarks, here are some setups that you can tune. Note that if you need more flexibility (let's say a custom transcoder), you still need to extend this driver and implement the facilities on your own.

You can set the following properties (with the default settings applied):

 - marklogic.url=http://127.0.0.1:8003 => The connection URL from one server.
.- marklogic.user => The user.
 - marklogic.password => The password of the user.
 - marklogic.json=true => Use json as target format. If false, xml cill be used.
 - marklogic.batchsize=1 => Use it for bulkwrite
 
 Marklogic HttpServer must be configure with basic authentification

