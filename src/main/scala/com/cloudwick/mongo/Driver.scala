package com.cloudwick.mongo

import com.cloudwick.mongo.dao.LogDAO
import org.slf4j.LoggerFactory
import com.cloudwick.generator.utils.Utils
import com.cloudwick.mongo.inserts.{BatchInsertConcurrent, InsertConcurrent}
import com.cloudwick.mongo.reads.ReadsConcurrent

/**
 * Driver for the mongo benchmark
 * @author ashrith 
 */
object Driver extends App {
  private val logger = LoggerFactory.getLogger(getClass)
  private val utils = new Utils

  /*
   * Command line option parser
   */
  val optionsParser = new scopt.OptionParser[OptionsConfig]("mongo_benchmark") {
    head("mongo", "0.6")
    opt[String]('m', "mode") required() valueName "<insert|read|agg_query>" action { (x, c) =>
      c.copy(mode = x)
    } validate { x: String =>
      if (x == "insert" || x == "read" || x == "agg_query")
        success
      else
        failure("value of '--mode' must be either 'insert', 'read' or 'agg_query'")
    } text "operation mode ('insert' will insert log events, 'read' will perform random reads" +
           " & 'agg_query' performs pre-defined aggregate queries)"
    opt[String]('u', "mongoURL") action { (x, c) =>
      c.copy(mongoURL = x)
    } text "mongo connection url to connect to, defaults to: 'mongodb://localhost:27017' (for more information on " +
           "mongo connection url format, please refer: http://goo.gl/UglKHb)"
    opt[Int]('e', "eventsPerSec") action { (x, c) =>
      c.copy(eventsPerSec = x)
    } text "number of log events to generate per sec"
    arg[Int]("<totalEvents>...") unbounded() optional() action { (x, c) =>
      c.copy(totalEvents = c.totalEvents :+ x)
    } text "total number of events to insert|read"
    opt[Int]('s', "ipSessionCount") action { (x, c) =>
      c.copy(ipSessionCount = x)
    } text "number of times a ip can appear in a session, defaults to: '25'"
    opt[Int]('l', "ipSessionLength") action { (x, c) =>
      c.copy(ipSessionLength = x)
    } text "size of the session, defaults to: '50'"
    opt[Int]('b', "batchSize") action { (x, c) =>
      c.copy(batchSize = x)
    } text "size of the batch to flush to mongo instead of single inserts, defaults to: '0'"
    opt[Int]('t', "threadCount") action { (x, c) =>
      c.copy(threadCount = x)
    } text "number of threads to use for write and read operations, defaults to: 1"
    opt[Int]('p', "threadPoolSize") action { (x, c) =>
      c.copy(threadPoolSize = x)
    } text "size of the thread pool, defaults to: 10"
    opt[String]('d', "dbName") action { (x, c) =>
      c.copy(mongoDbName = x)
    } text "name of the database to create|connect in mongo, defaults to: 'logs'"
    opt[String]('c', "collectionName") action { (x, c) =>
      c.copy(mongoCollectionName = x)
    } text "name of the collection to create|connect in mongo, defaults to: 'logEvents'"
    opt[String]('w', "writeConcern") action { (x, c) =>
      c.copy(writeConcern = x)
    } validate { x: String =>
      if (x == "none" || x == "safe" || x == "majority")
        success
      else
        failure("value of '--writeConcern' either 'none', 'safe' or 'majority'")
    } text "write concern level to use, possible values: none, safe, majority; defaults to: 'none'"
    opt[String]('r', "readPreference") action { (x, c) =>
      c.copy(readPreference = x)
    } validate { x: String =>
      if (x == "primary" || x == "primaryPreferred" || x == "secondary" || x == "secondaryPreferred" || x == "nearest")
        success
      else
        failure("value of '--readPreference' either 'primary', 'primaryPreferred', 'secondary', 'secondaryPreferred'" +
          "or 'nearest'")
    } text "read preference mode to use, possible values: primary, primaryPreferred, secondary, secondaryPreferred or " +
        "nearest; defaults to: 'none'\n" +
        "\t where,\n" +
        "\t\tprimary - all read operations use only current replica set primary\n" +
        "\t\tprimaryPreferred - if primary is unavailable fallback to secondary\n" +
        "\t\tsecondary - all read operations use only secondary members of the replica set\n" +
        "\t\tsecondaryPreferred - operations read from secondary members, fallback to primary\n" +
        "\t\tnearest - use this mode to read from both primaries and secondaries (may return stale data)"
    opt[Unit]('i', "indexData") action { (_, c) =>
      c.copy(indexData = true)
    } text "index data on 'response_code' and 'request_page' after inserting, defaults to: 'false'"
    opt[Unit]("shard") action  { (_, c) =>
      c.copy(shardMode = true)
    } text "specifies whether to create a shard collection or a normal collection"
    opt[Unit]("shardPreSplit") action  { (_, c) =>
      c.copy(shardPreSplit = true)
    } text "specifies whether to pre-split a shard and move the chunks to the available shards in the cluster"
    help("help") text "prints this usage text"
  }

  optionsParser.parse(args, OptionsConfig()) map { config =>
    logger.info(s"Successfully parsed command line args : $config")

    /*
     * Initialize mongo connection and initializes db and collection
     */
    val mongo = new LogDAO(config.mongoURL)
    val mongoClient = mongo.initialize
    val collection = mongo.initCollection(mongoClient, config.mongoDbName, config.mongoCollectionName)

    /*
     * Handles shutdown gracefully - close connection to mongo when exiting
     */
    sys.addShutdownHook({
      println()
      logger.info("ShutdownHook called - Closing connection with Mongo")
      mongo.close(mongoClient)
    })

    if(config.mode == "insert") {
      /*
       * Benchmark inserts
       */
      try {
        if (config.totalEvents.size == 0) {
          val eventsSize = 10000
          logger.info("Defaulting inserts to " + eventsSize)
          if (config.shardMode) {
            if (config.shardPreSplit) {
              logger.info("Dropping existing collection data")
              mongo.dropCollection(collection)
              logger.info("Initializing pre-splits and moving chunks around")
              utils.time("pre-splitting chunks") {
                mongo.initShardCollectionWithPreSplits(mongoClient,
                  config.mongoDbName,
                  config.mongoCollectionName,
                  eventsSize)
              }
            } else {
              logger.info("Dropping existing data in the collection and rebuilding sharded collection")
              mongo.dropCollection(collection)
              mongo.initShardCollection(mongoClient, config.mongoDbName, config.mongoCollectionName)
            }
          } else {
            logger.info("Dropping existing data in the collection and creating new collection")
            mongo.dropCollection(collection)
            mongo.initCollection(mongoClient, config.mongoDbName, config.mongoCollectionName)
          }
          if(config.batchSize == 0) {
            new InsertConcurrent(eventsSize, config, mongo).run()
          } else {
            new BatchInsertConcurrent(eventsSize, config, mongo).run()
          }
          if (config.indexData) {
            utils.time(s"indexing $eventsSize events") {
              mongo.createIndexes(collection, List("request_page", "response_code"))
            }
          }
        } else {
          config.totalEvents.foreach{ events =>
            if (config.shardMode) {
              if (config.shardPreSplit) {
                logger.info("Dropping existing collection data")
                mongo.dropCollection(collection)
                logger.info("Initializing pre-splits and moving chunks around")
                utils.time("pre-splitting chunks") {
                  mongo.initShardCollectionWithPreSplits(mongoClient,
                    config.mongoDbName,
                    config.mongoCollectionName,
                    events)
                }
              } else {
                logger.info("Dropping existing data in the collection and rebuilding sharded collection")
                mongo.dropCollection(collection)
                mongo.initShardCollection(mongoClient, config.mongoDbName, config.mongoCollectionName)
              }
            } else {
              logger.info("Dropping existing data in the collection and createing a new collection")
              mongo.dropCollection(collection)
              mongo.initCollection(mongoClient, config.mongoDbName, config.mongoCollectionName)
            }
            if (config.batchSize == 0) {
              new InsertConcurrent(events, config, mongo).run()
            } else {
              new BatchInsertConcurrent(events, config, mongo).run()
            }
            if (config.indexData) {
              utils.time(s"indexing $events events") {
                mongo.createIndexes(collection, List("record_id","request_page"))
              }
            }
          }
        }
      } catch {
        case e: Exception => logger.error("Oops! something went wrong " + e.printStackTrace()); System.exit(1)
      }
    } else if (config.mode == "read") {
      /*
       * Performs random reads
       */
      val collection = mongo.initCollection(mongoClient, config.mongoDbName, config.mongoCollectionName)
      // Set the read preference for the collection
      logger.info("Setting the read preference to " + config.readPreference)
      mongo.setReadPreference(collection, config.readPreference)
      // Get the count of events from mongo
      val totalDocuments = mongo.documentsCount(collection)
      logger.info("Total number of documents in the collection :" + totalDocuments)
      if (totalDocuments == 0) {
        logger.info("No documents found to read, please insert documents first using '--mode insert'")
        System.exit(0)
      }

      if(config.totalEvents.size == 0) {
        logger.info("Defaulting reads to 10000")
        new ReadsConcurrent(totalDocuments, 10000, config, mongo).run()
      } else {
        config.totalEvents.foreach { totalReads =>
          new ReadsConcurrent(totalDocuments, totalReads, config, mongo).run()
        }
      }
    } else {
      /*
       * Execute aggregation queries
       */
      val pipeline1 = mongo.buildQueryOne
      logger.info("Query 1 : Gets the number of times a status code has appeared")
      utils.time("aggregate query 1") {
        mongo.aggregationResult(mongoClient, config.mongoDbName, config.mongoCollectionName, pipeline1)
      }

      val pipeline2 = mongo.buildQueryTwo
      logger.info("Query 2: Co-relates request page to response code")
      utils.time("aggregate query 2") {
        mongo.aggregationResult(mongoClient, config.mongoDbName, config.mongoCollectionName, pipeline2)
      }

      val pipeline3 = mongo.buildQueryThree
      logger.info("Query 3: Counts total number of bytes served for each page by web server")
      utils.time("aggregate query 3") {
        mongo.aggregationResult(mongoClient, config.mongoDbName, config.mongoCollectionName, pipeline3)
      }

      val pipeline4 = mongo.buildQueryFour
      logger.info("Query 4: Counts how many times a client visited the site")
      utils.time("aggregate query 4") {
        mongo.aggregationResult(mongoClient, config.mongoDbName, config.mongoCollectionName, pipeline4)
      }

      val pipeline5 = mongo.buildQueryFive
      logger.info("Query 5: Top 10 site visitors")
      utils.time("aggregate query 5") {
        mongo.aggregationResult(mongoClient, config.mongoDbName, config.mongoCollectionName, pipeline5)
      }

      val pipeline6 = mongo.buildQuerySix
      logger.info("Query 5: Top Browsers")
      utils.time("aggregate query 6") {
        mongo.aggregationResult(mongoClient, config.mongoDbName, config.mongoCollectionName, pipeline6)
      }
    }
  } getOrElse {
    logger.error("Failed to parse command line arguments")
  }
}
