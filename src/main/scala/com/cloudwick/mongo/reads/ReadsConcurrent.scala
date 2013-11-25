package com.cloudwick.mongo.reads

import com.cloudwick.mongo.OptionsConfig
import com.cloudwick.mongo.dao.LogDAO
import org.slf4j.LoggerFactory
import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.atomic.AtomicLong
import com.cloudwick.generator.utils.Utils

/**
 * Performs random reads using pool of threads
 * @param totalDocuments count of total documents in the collection
 * @param events total number of read queries to build and execute
 * @param config scopt parsed command line options
 * @param mongo log event documents data access object
 * @author ashrith 
 */
class ReadsConcurrent (totalDocuments: Int, events: Long, config: OptionsConfig, mongo: LogDAO) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  val threadPool: ExecutorService = Executors.newFixedThreadPool(config.threadPoolSize)
  val finalCounter:AtomicLong = new AtomicLong(0L)
  val queriesPerThread = events / config.threadCount
  val utils = new Utils

  def run() = {
    utils.time(s"reading $events") {
      try {
        (1 to config.threadCount).foreach { threadCount =>
          logger.info(s"Initializing thread$threadCount")
          threadPool.execute(new Reads(queriesPerThread, totalDocuments, finalCounter, config, mongo))
        }
      } finally {
        threadPool.shutdown()
      }
      while(!threadPool.isTerminated) {}
      logger.info(s"Total read queries executed by ${config.threadCount} threads: " + finalCounter)
    }
  }
}