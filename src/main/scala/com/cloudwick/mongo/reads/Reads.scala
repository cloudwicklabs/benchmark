package com.cloudwick.mongo.reads

import org.slf4j.LoggerFactory
import com.cloudwick.generator.utils.Utils
import com.mongodb.casbah.Imports._
import java.util.concurrent.atomic.AtomicLong
import com.cloudwick.mongo.OptionsConfig
import com.cloudwick.mongo.dao.LogDAO
import scala.util.Random

/**
 * Performs Random Reads on a mongo collection
 * @param numOfReads number of read queries to perform
 * @param totalDocs total count of documents in the collection
 * @param counter atomic counter for keeping track of all threads read queries
 * @param config scopt parsed command line options
 * @param mongo log event documents data access object
 * @author ashrith 
 */
class Reads(numOfReads: Long,
            totalDocs: Int,
            counter: AtomicLong,
            config: OptionsConfig,
            mongo: LogDAO) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val utils = new Utils

  def threadName = Thread.currentThread().getName

  def run() = {
    val mongoClient = mongo.initialize
    val collection = mongo.initCollection(mongoClient, config.mongoDbName, config.mongoCollectionName)
    try {
      utils.time(s"reading $numOfReads by thread $threadName") {
        (1L to numOfReads).foreach { _ =>
          mongo.findDocument(collection, MongoDBObject("_id" -> Random.nextInt(totalDocs)))
        }
        logger.info(s"Read queries executed by $threadName is: $numOfReads")
      }
      counter.getAndAdd(numOfReads)
    } finally {
      mongo.close(mongoClient)
    }
  }
}
