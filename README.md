Benchmark NoSQL
===============

This project is intended to test write, read and query performances of several NoSQL data-stores like MongoDB, Cassandra,
HBase, Redis, Solr and various others.

**Supported**: MongoDB, Solr, Cassandra

**WIP**: HBase

Getting the Project
-------------------

There are two methods to build this project 

1. Manual Build 
2. Download Jar file 

For Manual Build follow the below steps 

Method 1 
--------
Clone the project using git:

```
cd /opt
git clone https://github.com/cloudwicklabs/benchmark.git
```

Build Project
-------------
Requirement for building the project:

 * [sbt](http://www.scala-sbt.org/) (version 0.13.0)
 * [sbt-assembly](https://github.com/sbt/sbt-assembly)

Once, requirements are installed. Follow these steps to build a jar with dependencies (run the following command from
projects root)

```
cd /opt/benchmark
sbt assembly
```

Running
-------
To run the benchmark programs, use the wrapper script

```
cd /opt/benchmark
chmod +x bin/benchmark
bin/benchmark
```

with out any arguments, `run` script will show the supported sub-commands that a user can run:

```
Usage: benchmark DRIVER
Possible DRIVER(s)
  mongo       mongo benchmark driver
  solr        solr benchmark driver
  cassandra   cassandra benchmark driver
```
where, each sub-command represents a driver interface to the application that's used to benchmark.

###Benchmarking Mongo
Mongo benchmark driver can do the following:

  * Benchmark inserts for given number of events (also supports concurrency)
  * Benchmark random reads (also supports concurrency)
  * Benchmark predefined aggregate queries on top of inserted data

```
$bin/benchmark mongo --help

mongo 0.7
Usage: mongo_benchmark [options] [<totalEvents>...]

  -m <insert|read|agg_query> | --mode <insert|read|agg_query>
        operation mode ('insert' will insert log events, 'read' will perform random reads & 'agg_query' performs pre-defined aggregate queries)
  -u <value> | --mongoURL <value>
        mongo connection url to connect to, defaults to: 'mongodb://localhost:27017' (for more information on mongo connection url format, please refer: http://goo.gl/UglKHb)
  -e <value> | --eventsPerSec <value>
        number of log events to generate per sec
  <totalEvents>...
        total number of events to insert|read
  -s <value> | --ipSessionCount <value>
        number of times a ip can appear in a session, defaults to: '25'
  -l <value> | --ipSessionLength <value>
        size of the session, defaults to: '50'
  -b <value> | --batchSize <value>
        size of the batch to flush to mongo instead of single inserts, defaults to: '0'
  -t <value> | --threadsCount <value>
        number of threads to use for write and read operations, defaults to: 1
  -p <value> | --threadPoolSize <value>
        size of the thread pool, defaults to: 10
  -d <value> | --dbName <value>
        name of the database to create|connect in mongo, defaults to: 'logs'
  -c <value> | --collectionName <value>
        name of the collection to create|connect in mongo, defaults to: 'logEvents'
  -w <value> | --writeConcern <value>
        write concern level to use, possible values: none, safe, majority; defaults to: 'none'
  -r <value> | --readPreference <value>
        read preference mode to use, possible values: primary, primaryPreferred, secondary, secondaryPreferred or nearest; defaults to: 'none'
	 where,
		primary - all read operations use only current replica set primary
		primaryPreferred - if primary is unavailable fallback to secondary
		secondary - all read operations use only secondary members of the replica set
		secondaryPreferred - operations read from secondary members, fallback to primary
		nearest - use this mode to read from both primaries and secondaries (may return stale data)
  -i | --indexData
        index data on 'response_code' and 'request_page' after inserting, defaults to: 'false'
  --shard
        specifies whether to create a shard collection or a normal collection
  --shardPreSplit
        specifies whether to pre-split a shard and move the chunks to the available shards in the cluster
  -o <value> | --operationRetries <value>
        number of times a operation has to retired before exhausting, defaults to: '10'
  --help
        prints this usage text
```


Mongo Benchmark Example(s):

1. Inserts of 100000, 1000000 and 100000000 documents consecutively on single mongo instance:

    ```
    bin/benchmark mongo --mode insert 100000 1000000 100000000
    ```

2. Inserts of 100000, 1000000 and 100000000 documents on sharded mongo cluster, this requires running the benchmark
 from `mongos` (mongo router)

    ```
    bin/benchmark mongo --mode insert 100000 1000000 100000000 --shard
    ```

3. Inserts of 100000, 1000000 and 100000000 documents consecutively and also indexes the data once inserted:

    ```
    bin/benchmark mongo --mode insert 100000 1000000 100000000 --indexData
    ```

4. Insert data with indexing and custom batch size:

    ```
    bin/benchmark mongo --mode insert 100000 1000000 100000000 --indexData --batchSize 5000
    ```

5. Benchmark random reads of 10000, 100000 and 1000000 documents:

    ```
    bin/benchmark mongo --mode read 10000 100000 1000000
    ```

6. Perform aggregation queries on the inserted data

    ```
    bin/benchmark mongo --mode agg_query
    ```

>
> All the inserts and reads can be run concurrently using specified number of threads and threadPools, see options for
> more details
>

**NOTE**: By default, mongo benchmark driver tries to connect to local instance of mongo, please use `--mongoURL` to
specify the path where the mongo is listening. For more details on how to build the url please visit
[here](http://goo.gl/UglKHb).

Example connection URI scheme:

```
mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
```

###Benchmarking Solr
Solr benchmark driver can do the following:

  * Benchmark inserts for a given range of inputs
  * Benchmark random reads
  * Benchmark custom user input queries

```
$bin/benchmark solr --help

solr 0.1
Usage: index_logs [options] [<totalEvents>...]

Indexes log events to solr
  -m <index|read|search> | --mode <index|read|search>
        operation mode ('index' will index logs, 'search' will perform search &'read' will perform random reads against solr core)
  -u <value> | --solrServerUrl <value>
        url for connecting to solr server instance
  <totalEvents>...
        total number of events to insert
  -s <value> | --ipSessionCount <value>
        number of times a ip can appear in a session
  -l <value> | --ipSessionLength <value>
        size of the session
  -b <value> | --batchSize <value>
        flushes events from memory to solr index, defaults to: '1000'
  -c | --cleanPreviousIndex
        deletes the existing index on solr core
  -q <value> | --query <value>
        solr query to execute, defaults to: '*:*'
  --queryCount <value>
        number of documents to return on executed query, default:10
  --help
        prints this usage text
```

Before, benchmarking Solr you have to create a solr collection|core named `logs`. The following steps illustrate how to
create a collection named logs on single solr core instance:

```
cp -r $SOLR_HOME/example $SOLR_HOME/logs
cp -r $SOLR_HOME/tweets/solr/collection1 $SOLR_HOME/tweets/solr/logs
rm -r $SOLR_HOME/tweets/solr/collection1
echo 'name=logs' > $SOLR_HOME/tweets/solr/logs/core.properties
```
*where, `$SOLR_HOME` is path where solr is installed (unpacked).*

Also,

  * `schema.xml` from logs collection should be replaced with `resources/schema.xml` from the project dir
  * `solrconfig.xml` from logs collection should be replaced with `resources/solrconfig.xml` from the project dir

Solr Driver Example(s):

1. To benchmark the inserts of 100000, 1000000 and 100000000 log events consecutively:

    ```
    bin/benchmark solr --mode insert 100000 1000000 100000000
    ```

2. To benchmark the inserts of 100000, 1000000 and 100000000 log events consecutively while clearing existing index data:

    ```
    bin/benchmark solr --mode insert 100000 1000000 100000000 --cleanPreviousIndex
    ```

3. To benchmark the inserts of 100000, 1000000 and 100000000 log events consecutively while clearing existing index data and
also providing batch size using which the driver will flush the data:

    ```
    bin/benchmark solr --mode insert 100000 1000000 100000000 --cleanPreviousIndex --batchSize 10000
    ```

4. To benchmark random reads

    ```
    bin/benchmark solr --mode read 10000 100000 1000000
    ```

5. To execute custom queries and return specified number of documents

    ```
    bin/benchmark solr --mode search --query '*:*' --queryCount 20
    ```

###Benchmarking Cassandra
Cassandra benchmark driver can do the following:

  * Benchmark inserts for a given range of inputs
      * Generates random movie dataset events
      * Can insert in both `batch` mode and `normal` mode, batch mode automatically batches a set of events and sends them to cassandra
      * supports both `sync` and `async` operations, *async* does not wait for the cassandra to give the ack back
      * supports operation retries, default to 10 retries for operation before exhausting
  * Benchmark random reads
      * Automatically builds random queries to query the data from inserted 4 tables

```
$bin/benchmark cassandra --help

cassandra 0.5
Usage: cassandra_benchmark [options] [<totalEvents>...]

  -m <insert|read|query> | --mode <insert|read|query>
        operation mode ('insert' will insert log events, 'read' will perform random reads & 'query' performs pre-defined set of queries on the inserted data set)
  -n <value> | --cassNode <value>
        cassandra node to connect, defaults to: '127.0.0.1'
  <totalEvents>...
        total number of events to insert|read
  -b <value> | --batchSize <value>
        size of the batch to flush to cassandra; set this to avoid single inserts, defaults to: '0'
  -c <value> | --customersDataSize <value>
        size of the data set of customers to use for generating data, defaults to: '1000'
  -k <value> | --keyspaceName <value>
        name of the database to create|connect in cassandra, defaults to: 'moviedata'
  -d | --dropExistingTables
        drop existing tables in the keyspace, defaults to: 'false'
  -a | --aSyncInserts
        performs asynchronous inserts, defaults to: 'false'
  -r <value> | --replicationFactor <value>
        replication factor to use when inserting data, defaults to: '1'
  -o <value> | --operationRetries <value>
        number of times a operation has to retired before exhausting, defaults to: '10'
  -t <value> | --threadsCount <value>
        number of threads to use for write and read operations, defaults to: 1
  -p <value> | --threadPoolSize <value>
        size of the thread pool, defaults to: 10
  --help
        prints this usage text
```
Cassandra Benchmark Example(s):

1. Inserts of 2500, 25000, 250000 of rows into 4 tables which is equivalent to 10000, 100000, 1000000 insertions

    ```
    bin/benchmark cassandra --mode insert 2500 25000 250000
    ```
2. Inserts of 2500, 25000, 250000 of rows into 4 tables using batch inserts with a batch size of 500

    ```
    bin/benchmark cassandra --mode insert 2500 25000 250000 --batchSize 500
    ```
3. Inserts of 2500, 25000, 250000 of rows into 4 tables using batch inserts with a batch size of 500 and also delete the
previously existing data in the tables:

    ```
    bin/run cassandra --mode insert 2500 25000 250000 --batchSize 500 --dropExistingTables
    ```
4. Inserts in asynchronous mode, which does not required acknowledge back from cassandra:

    ```
    bin/run cassandra --mode insert 2500 25000 250000 --batchSize 500 --dropExistingTables --aSyncInserts
    ```
5. Random reads

    ```
    bin/run cassandra --mode read 2500 25000 250000
    ```

>
> All inserts and read operations in cassandra benchmark driver can be made concurrent by specifying theads count and
> thread poolsize, see options for more details
>


For the Download Jar method follow the below steps 

Method 2
---------

Download the jar from githud downloads 
    
	```
	wget https://github.com/cloudwicklabs/benchmark
	```

Copy the file to tmp location 

java -cp benchmark.jar /tmp 



###License and Authors
Authors: [Ashrith](http://github.com/ashrithr)

Apache 2.0. Please see `LICENSE.txt`. All contents copyright (c) 2013, Cloudwick Labs.
