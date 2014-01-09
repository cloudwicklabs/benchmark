package com.cloudwick.cassandra

import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import com.cloudwick.cassandra.inserts.{BatchInsertConcurrent, InsertConcurrent}
import com.cloudwick.cassandra.reads.ReadsConcurrent

/**
 * Driver interface for cassandra benchmark
 * @author ashrith 
 */
object Driver extends App {
  private val logger = LoggerFactory.getLogger(getClass)

  /*
   * Command line option parser
   */
  val optionsParser = new scopt.OptionParser[OptionsConfig]("cassandra_benchmark") {
    head("cassandra", "0.4")
    opt[String]('m', "mode") required() valueName "<insert|read|query>" action { (x, c) =>
      c.copy(mode = x)
    } validate { x: String =>
      if (x == "insert" || x == "read" || x == "query")
        success
      else
        failure("value of '--mode' must be either 'insert', 'read' or 'query'")
    } text "operation mode ('insert' will insert log events, 'read' will perform random reads" +
      " & 'query' performs pre-defined set of queries on the inserted data set)"
    opt[String]('n', "cassNode") unbounded() action { (x, c) =>
      c.copy(cassandraNode = c.cassandraNode :+ x)
    } text "cassandra node to connect, defaults to: '127.0.0.1'"
    arg[Int]("<totalEvents>...") unbounded() optional() action { (x, c) =>
      c.copy(totalEvents = c.totalEvents :+ x)
    } text "total number of events to insert|read"
    opt[Int]('b', "batchSize") action { (x, c) =>
      c.copy(batchSize = x)
    } text "size of the batch to flush to cassandra; set this to avoid single inserts, defaults to: '0'"
    opt[Int]('c', "customersDataSize") action { (x, c) =>
      c.copy(customerDataSetSize = x)
    } text "size of the data set of customers to use for generating data, defaults to: '1000'"
    opt[String]('k', "keyspaceName") action { (x,c) =>
      c.copy(keyspaceName = x)
    } text "name of the database to create|connect in cassandra, defaults to: 'moviedata'"
    opt[Unit]('d', "dropExistingTables") action { (_, c) =>
      c.copy(dropExistingTables = true)
    } text "drop existing tables in the keyspace, defaults to: 'false'"
    opt[Unit]('a', "aSyncInserts") action { (_, c) =>
      c.copy(aSync = true)
    } text "performs asynchronous inserts, defaults to: 'false'"
    opt[Int]('r', "replicationFactor") action { (x, c) =>
      c.copy(replicationFactor = x)
    } text "replication factor to use when inserting data, defaults to: '1'"
    opt[Int]('o', "operationRetries") action { (x, c) =>
      c.copy(operationRetires = x)
    } text "number of times a operation has to retired before exhausting, defaults to: '10'"
    opt[Int]('t', "threadsCount") action { (x, c) =>
      c.copy(threadCount = x)
    } text "number of threads to use for write and read operations, defaults to: 1"
    opt[Int]('p', "threadPoolSize") action { (x, c) =>
      c.copy(threadPoolSize = x)
    } text "size of the thread pool, defaults to: 10"
    help("help") text "prints this usage text"
  }

  optionsParser.parse(args, OptionsConfig()) map { config =>
    logger.info("Successfully parsed command line options: {}", config)

    /*
     * Initialize data generators and cassandra connection
     */
    val movieDAO = new com.cloudwick.cassandra.dao.MovieDAO(config.cassandraNode)
    val tableList = Seq("watch_history", "customer_rating", "customer_queue", "movies_genre")

    /*
     * Handles graceful shutdowns - close connection and session objects to cassandra
     */
    sys.addShutdownHook({
      println()
      logger.info("ShutdownHook called - Closing connection with Cassandra")
      movieDAO.close()
    })

    if(config.mode == "insert") {
      /*
       * Benchmark Inserts
       */
      try {
        if (config.dropExistingTables) {
          tableList.foreach { table =>
            logger.info("Dropping table: {}", table)
            movieDAO.dropTable(config.keyspaceName, table)
          }
        }

        logger.info("Creating keyspace {}", config.keyspaceName)
        movieDAO.createSchema(config.keyspaceName, config.replicationFactor)

        logger.info("Initializing Data Generator")

        if(config.totalEvents.size == 0) {
          val events = 10000
          logger.info("Defaulting inserts to {}", events)
          if (config.batchSize == 0) {
            new InsertConcurrent(events, config).run()
          } else {
            new BatchInsertConcurrent(events, config).run()
          }
          sys.exit(0)
        } else {
          config.totalEvents.foreach { events =>
            if (config.batchSize == 0) {
              new InsertConcurrent(events, config).run()
            } else {
              new BatchInsertConcurrent(events, config).run()
            }
          }
        }
        sys.exit(0)
      } catch {
        case e: Exception => logger.error("Oops! something went wrong, reason: {}", e.printStackTrace())
      } finally {
        movieDAO.close()
      }
    } else if (config.mode == "read") {
      /*
       * Performs random reads on inserted data
       */
      logger.info("Initializing random reads")
      if (config.totalEvents.size == 0) {
        logger.info("Defaulting reads to 1000")
        new ReadsConcurrent(1000, config).run()
        sys.exit(0)
      } else {
        config.totalEvents.foreach{ totalReads =>
          new ReadsConcurrent(totalReads, config).run()
        }
        sys.exit(0)
      }
    } else {
      /*
       * Execute pre-defined queries on inserted data
       */
      logger.error("This part is not implemented")
      sys.exit(1)
    }
  }
}
