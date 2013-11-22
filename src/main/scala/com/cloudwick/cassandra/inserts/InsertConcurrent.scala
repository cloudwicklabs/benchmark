package com.cloudwick.cassandra.inserts

import org.slf4j.LoggerFactory
import com.cloudwick.generator.utils.Utils
import com.cloudwick.cassandra.OptionsConfig
import com.cloudwick.generator.movie.{MovieGenerator, Person}
import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.atomic.AtomicLong


/**
 * Description goes here
 * @author ashrith 
 */
class InsertConcurrent(events: Long, config: OptionsConfig) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  val threadPool: ExecutorService = Executors.newFixedThreadPool(config.threadPoolSize)
  val finalCounter: AtomicLong = new AtomicLong(0L)
  val messagesPerThread: Int = (events / config.threadCount).toInt
  val messagesRange = Range(0, events.toInt, messagesPerThread)

  val utils = new Utils
  val person = new Person
  val customers = scala.collection.mutable.Map[Int, String]()
  val tableList = Seq("watch_history", "customer_rating", "customer_queue", "movies_genre")


  def buildCustomersMap = {
    logger.info("Building a customer data set of size: " + config.customerDataSetSize)
    (1 to config.customerDataSetSize).foreach { custId =>
      customers += custId -> person.gen
    }
    customers.toMap
  }

  def run() = {
    utils.time(s"inserting $events") {
      try {
        (1 to config.threadCount).foreach { threadCount =>
          logger.info("Initializing thread" + threadCount)
          threadPool.execute(
            new Insert(
              messagesRange(threadCount-1), // start range for thread
              messagesRange(threadCount-1) + (messagesPerThread - 1), // end range for thread
              finalCounter,
              buildCustomersMap, // send immutable map to each thread
              config
            )
          )
        }
      } finally {
        threadPool.shutdown()
      }
      while(!threadPool.isTerminated) {}
      logger.info(s"Total documents processed by ${config.threadCount} threads: $finalCounter")
    }
  }
}
