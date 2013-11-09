package com.cloudwick.solr

import com.cloudwick.solr.dao.{IndexLogs,SearchLogs}
import com.cloudwick.generator.log.{IPGenerator,LogGenerator}
import org.slf4j.LoggerFactory
import scala.util.Random

/**
 * Driver for the solr benchmark
 * @author ashrith 
 */
object Driver extends App {
  private val logger = LoggerFactory.getLogger(getClass)

  val optionsParser = new scopt.OptionParser[OptionsConfig]("index_logs") {
    head("solr", "0.1")
    note("Indexes log events to solr")
    opt[String]('m', "mode") required() valueName "<index|read|search>" action { (x, c) =>
      c.copy(mode = x)
    } validate { x:String =>
      if (x == "index" || x == "search" || x == "read")
        success
      else
        failure("value of '--mode' must be either 'index' or 'search' or 'read'")
    } text "operation mode ('index' will index logs, 'search' will perform search &" +
           "'read' will perform random reads against solr core)"
    opt[String]('u', "solrServerUrl") action { (x, c) =>
      c.copy(solrServerUrl = x)
    } text "url for connecting to solr server instance"
    arg[Int]("<totalEvents>...") unbounded() optional() action { (x, c) =>
      c.copy(totalEvents = c.totalEvents :+ x)
    } text "total number of events to insert|read"
    opt[Int]('s', "ipSessionCount") action { (x, c) =>
      c.copy(ipSessionCount = x)
    } text "number of times a ip can appear in a session, defaults to: '25'"
    opt[Int]('l', "ipSessionLength") action { (x, c) =>
      c.copy(ipSessionLength = x)
    } text "size of the session, defaults to: 50"
    opt[Int]('b', "batchSize") action { (x, c) =>
      c.copy(batchSize = x)
    } text "flushes events from memory to solr index, defaults to: '1000'"
    opt[Unit]('c', "cleanPreviousIndex") action { (_, c) =>
      c.copy(cleanPreviousIndex = true)
    } text "deletes the existing index on solr core"
    opt[String]('q', "query") action { (x, c) =>
      c.copy(solrQuery = x)
    } text "solr query to execute, defaults to: '*:*'"
    opt[Int]("queryCount") action { (x, c) =>
      c.copy(queryCount = x)
    } text "number of documents to return on executed query, default:10"
    help("help") text "prints this usage text"
  }

  optionsParser.parse(args, OptionsConfig()) map { config =>
    logger.info(s"Successfully parsed command line args : $config")
    val indexer = new IndexLogs(config.solrServerUrl)
    val searcher = new SearchLogs(config.solrServerUrl)

    /*
     * shutdown hook to handle CTRL+C
     */
    sys.addShutdownHook({
      println()
      logger.info("ShutdownHook called")
      if(config.mode == "index") {
        logger.info("committing to solr index")
        indexer.commitDocuments
      }
    })

    if (config.mode == "index") {
      /*
       * Benchmark inserts
       */
      try {
        if (config.totalEvents.size == 0) {
          logger.info("Defaulting inserts to 1000")
          indexLogs(indexer,
            1000,
            config.batchSize,
            config.ipSessionCount,
            config.ipSessionLength,
            config.cleanPreviousIndex
          )
        } else {
          config.totalEvents.foreach { totalEvents =>
            indexLogs(indexer,
              totalEvents,
              config.batchSize,
              config.ipSessionCount,
              config.ipSessionLength,
              config.cleanPreviousIndex
            )
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        println()
        logger.info("committing to solr index")
        indexer.commitDocuments
      }
    } else if (config.mode == "search") {
      searchLogs(searcher, config.solrQuery, config.queryCount)
    } else {
      /*
       * Benchmark random reads
       */
      if (config.totalEvents.size == 0) {
        logger.info("Defaulting random reads to 1000")
        readLogs(searcher, 1000)
      } else {
        config.totalEvents.foreach { totalReads =>
          readLogs(searcher, totalReads)
        }
      }
    }
  } getOrElse {
    logger.error("Failed to parse command line args")
  }

  /**
   * Generates mock log events and indexes them to solr index
   * @param indexer IndexLogs object
   * @param totalEvents total events to insert
   * @param batchSize size of the batches for which solr to flush events to disk
   * @param sessionCount IP session count, used for generating events
   * @param sessionLength IP session length, used for generating events
   * @param cleanPreviousIndex flag to specify whether to clean existing index on solr
   */
  def indexLogs(indexer: IndexLogs,
                totalEvents: Int,
                batchSize: Int,
                sessionCount: Int,
                sessionLength: Int,
                cleanPreviousIndex:Boolean) = {
    logger.info("Generating log events " +
      "with session count of: " + Console.BLUE + sessionCount + Console.WHITE +
      " and session length of: " + Console.BLUE + sessionLength + Console.RESET)
    logger.info("Indexing log events to Solr server @ " + Console.BLUE + indexer.server.getBaseURL + Console.RESET)
    logger.info("Press CTRL+C to stop generating events and commit them to solr index")

    val ipGenerator = new IPGenerator(sessionCount, sessionLength)
    val logEventGen = new LogGenerator(ipGenerator)
    var messagesCount = 0
    var totalMessagesCount = 0

    // clean existing index if user sets the flag
    if(cleanPreviousIndex) {
      logger.info("Cleaning up existing index")
      indexer.cleanup
    }

    time(s"inserting $totalEvents") {
      (1 to totalEvents).foreach { _ =>
        messagesCount += 1
        totalMessagesCount += 1
        printf("\rLogEvents indexed: " + totalMessagesCount)
        indexer.indexDocument(logEventGen.eventGenerate, totalMessagesCount)
        if (messagesCount == batchSize || messagesCount == totalEvents) {
          logger.debug("Flushing (committing documents to solr index) batch size of " + batchSize)
          indexer.commitDocuments
          messagesCount = 0 // reset the batchSize counter
        }
      }
      println()
    }
  }

  /**
   * Performs random reads by record_id
   * @param searcher SearchLogs object
   * @param totalReads number of reads to perform
   */
  def readLogs(searcher: SearchLogs, totalReads: Int) = {
    time(s"reading $totalReads") {
      val totalDocuments = searcher.getCount
      (1 to totalReads).foreach { eventCount =>
        printf("\rLogEvents retrieved: " + eventCount)
        searcher.querySingleDocument(s"record_id:${Random.nextInt(totalDocuments.toInt)}").getResultAsMap()
      }
      println()
    }
  }

  /**
   * Search solr using a specified query
   * @param searcher SearchLogs object
   * @param solrQuery Solr query to execute
   * @param queryCount Throttle the output count from the query
   */
  def searchLogs(searcher :SearchLogs, solrQuery:String, queryCount:Int) = {
    logger.info("Querying solr with query: " + Console.BLUE + solrQuery + Console.RESET)
    val results = searcher.executeQuery(solrQuery, queryCount)
    results.documents.foreach { logEvent: Map[String, Any] =>
      searcher.prettyPrint(logEvent)
    }
  }

  /**
   * Measures time took to run a block
   * @param block code block to run
   * @param message additional message to print
   * @tparam R type
   * @return returns block output
   */
  def time[R](message: String = "code block")(block: => R): R = {
    val s = System.nanoTime
    // block: => R , implies call by name i.e, the execution of block is delayed until its called by name
    val ret = block
    logger.info("Time elapsed in " + message + " : " +(System.nanoTime - s)/1e6+"ms")
    ret
  }
}
