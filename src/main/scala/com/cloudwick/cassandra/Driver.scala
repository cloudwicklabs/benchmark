package com.cloudwick.cassandra

import com.cloudwick.generator.movie.{Customers, Person, MovieGenerator}
import com.cloudwick.generator.utils.Utils
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util
import org.slf4j.LoggerFactory

/**
 * Driver interface for cassandra benchmark
 * @author ashrith 
 */
object Driver extends App {
  private val logger = LoggerFactory.getLogger(getClass)
  private val utils = new Utils

  /*
   * Command line option parser
   */
  val optionsParser = new scopt.OptionParser[OptionsConfig]("cassandra_benchmark") {
    head("cassandra", "0.1")
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
      c.copy(async = true)
    } text "performs asynchronous inserts, defaults to: 'false'"
    opt[Int]('r', "replicationFactor") action { (x, c) =>
      c.copy(replicationFactor = x)
    } text "replication factor to use when inserting data, defaults to: '1'"
    help("help") text "prints this usage text"
  }

  optionsParser.parse(args, OptionsConfig()) map { config =>
    logger.info(s"Successfully parsed command line options: $config")

    /*
     * Initialize data generators and cassandra connection
     */
    val movie = new MovieGenerator
    val person = new Person
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
        // customers data bag
        val customers = scala.collection.mutable.Map[Int, String]()
        // Batch insert data holders
        val customerWatchHistoryData = new util.HashMap[Integer, util.List[String]]()
        val customerRatingData = new util.HashMap[Integer, util.List[String]]()
        val customerQueueData = new util.HashMap[Integer, util.List[String]]()
        val movieGenreData = new util.HashMap[Integer, util.List[String]]()
        // counters for keeping track of batches and total inserts
        var messagesCount = 0
        var totalMessagesCount = 0

        logger.info("Building a customer data set of size: " + config.customerDataSetSize)
        (1 to config.customerDataSetSize).foreach { custId =>
          customers += custId -> person.gen
        }

        if (config.dropExistingTables) {
          tableList.foreach { table =>
            logger.info(s"Dropping table: $table")
            movieDAO.dropTable(config.keyspaceName, table)
          }
        }

        logger.info(s"Creating keyspace ${config.keyspaceName}")
        movieDAO.createSchema(config.keyspaceName, config.replicationFactor)

        logger.info("Initializing Data Generator")
        /*
         * Anonymous function to insert data
         */
        val benchmarkInserts = (events: Int) => {
          utils.time(s"inserting $events") {
            val customerSize = customers.size

            (1 to events).foreach { recordID =>
              val cId: Int = Random.nextInt(customerSize) + 1
              val cName: String = customers(cId)
              val movieInfo: Array[String] = movie.gen
              val customer: Customers = new Customers(cId, cName, movieInfo(3).toInt)

              val customerID: Int               = customer.custId
              val customerName: String          = customer.custName
              val customerActive: String        = customer.userActiveOrNot
              val customerTimeWatchedInit: Long = customer.timeWatched
              val customerPausedTime: Int       = customer.pausedTime
              val customerRating: String        = customer.rating
              val movieId: String               = movieInfo(0)
              val movieName: String             = movieInfo(1).replace("'", "")
              val movieReleaseYear: String      = movieInfo(2)
              val movieRunTime: String          = movieInfo(3)
              val movieGenre: String            = movieInfo(4)

              if (config.batchSize != 0) {
                // Batch Inserts
                customerWatchHistoryData.put(recordID, bufferAsJavaList(ArrayBuffer(
                  customerID.toString,
                  customerTimeWatchedInit.toString,
                  customerPausedTime.toString,
                  movieId,
                  movieName)))
                customerRatingData.put(recordID, bufferAsJavaList(ArrayBuffer(
                  customerID.toString,
                  movieId,
                  movieName,
                  customerName,
                  customerRating)))
                customerQueueData.put(recordID, bufferAsJavaList(ArrayBuffer(
                  customerID.toString,
                  customerTimeWatchedInit.toString,
                  customerName,
                  movieId,
                  movieName)))
                movieGenreData.put(recordID, bufferAsJavaList(ArrayBuffer(
                  movieGenre,
                  movieReleaseYear,
                  movieId,
                  movieRunTime,
                  movieName)))
                messagesCount += 1
                totalMessagesCount += 1
                printf("\rMessages Count: " + totalMessagesCount)
                if (messagesCount == config.batchSize || messagesCount == events) {
                  logger.debug("Flushing batch size of :" + config.batchSize)
                  movieDAO.batchLoadWatchHistory(config.keyspaceName, customerWatchHistoryData, config.async)
                  movieDAO.batchLoadCustomerRatings(config.keyspaceName, customerRatingData, config.async)
                  movieDAO.batchLoadCustomerQueue(config.keyspaceName, customerQueueData, config.async)
                  movieDAO.batchLoadMovieGenre(config.keyspaceName, movieGenreData, config.async)
                  messagesCount = 0
                  customerWatchHistoryData.clear()
                  customerRatingData.clear()
                  customerQueueData.clear()
                  movieGenreData.clear()
                }
              }
              else {
                // Individual Inserts
                movieDAO.loadWatchHistory(config.keyspaceName,
                  customerID,
                  customerTimeWatchedInit.toString,
                  customerPausedTime,
                  movieId.toInt,
                  movieName,
                  config.async)
                movieDAO.loadCustomerRatings(config.keyspaceName,
                  customerID,
                  movieId.toInt,
                  movieName,
                  customerName,
                  customerRating.toFloat,
                  config.async
                )
                movieDAO.loadCustomerQueue(config.keyspaceName,
                  customerID,
                  customerTimeWatchedInit.toString,
                  customerName,
                  movieId.toInt,
                  movieName,
                  config.async)
                movieDAO.loadMovieGenre(config.keyspaceName,
                  movieGenre,
                  movieReleaseYear.toInt,
                  movieId.toInt,
                  movieRunTime.toInt,
                  movieName,
                  config.async)
                totalMessagesCount += 1
                printf("\rMessages Count: " + totalMessagesCount)
              }

            }
            println()
          }
          totalMessagesCount = 0
        }

        if(config.totalEvents.size == 0) {
          logger.info("Defaulting inserts to 1000")
          benchmarkInserts(1000)
          sys.exit(0)
        } else {
          config.totalEvents.foreach { events =>
            benchmarkInserts(events)
          }
          sys.exit(0)
        }
      } catch {
        case e: Exception => logger.error("Oops! something went wrong " + e.printStackTrace())
      }
    } else if (config.mode == "read") {
      /*
       * Performs random reads on inserted data
       */
      val benchmarkReads = (numberOfReads: Int) => {
        utils.time(s"reading $numberOfReads") {
          logger.info("Using total number of customers data set: " + config.customerDataSetSize)
          logger.info("Building query sets")
          val movieInfo: Array[String] = movie.gen
          val querySet = Map(
            "query1" -> new String(s"SELECT movie_name, pt, ts FROM ${config.keyspaceName}.watch_history " +
              "WHERE cid=CUSTID;"),
            "query2" -> new String(s"SELECT movie_name, customer_name, rating " +
              s"FROM ${config.keyspaceName}.customer_rating WHERE cid=CUSTID;"),
            "query3" -> new String(s"SELECT customer_name, mid, movie_name " +
              s"FROM ${config.keyspaceName}.customer_queue WHERE cid=CUSTID;"),
            "query4" -> new String(s"SELECT movie_name, duration " +
              s"FROM ${config.keyspaceName}.movies_genre WHERE genre='GENRE' AND release_year=RYEAR AND mid=MID;")
          )
          (1 to numberOfReads).foreach { readQueryCount =>
            val randomQuery: String            = s"query${Random.nextInt(4) + 1}"
            val movieId: String               = movieInfo(0)
            val movieName: String             = movieInfo(1).replace("'", "")
            val movieReleaseYear: String      = movieInfo(2)
            val movieRunTime: String          = movieInfo(3)
            val movieGenre: String            = movieInfo(4)
            val query: String = randomQuery match {
              case "query1" => querySet.get(randomQuery).get
                .replaceAll("CUSTID", (Random.nextInt(config.customerDataSetSize)+ 1).toString)
              case "query2" => querySet.get(randomQuery).get
                .replaceAll("CUSTID", (Random.nextInt(config.customerDataSetSize)+ 1).toString)
              case "query3" => querySet.get(randomQuery).get
                .replaceAll("CUSTID", (Random.nextInt(config.customerDataSetSize)+ 1).toString)
              case "query4" => querySet.get(randomQuery).get
                .replaceAll("GENRE", movieGenre)
                .replaceAll("RYEAR", movieReleaseYear)
                .replaceAll("MID", movieId)
            }
            try {
              movieDAO.findCQLByQuery(query)
            } catch {
              case ex: Exception => logger.warn(s"Failed executing query: '$query' reason: $ex")
            }
          }
        }
      }

      logger.info("Init random reads")
      if (config.totalEvents.size == 0) {
        logger.info("Defaulting reads to 100")
        benchmarkReads(100)
        sys.exit(0)
      } else {
        config.totalEvents.foreach{ totalReads =>
          benchmarkReads(totalReads)
        }
        sys.exit(0)
      }
    } else {
      /*
       * Execute pre-defined queries on inserted data
       */
      logger.info("This part is not implemented")
      sys.exit(1)
    }
  }
}
