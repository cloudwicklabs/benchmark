package com.cloudwick.mongo.inserts

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import com.cloudwick.generator.utils.Utils
import com.cloudwick.generator.log.{LogGenerator, IPGenerator}
import com.cloudwick.mongo.dao.LogDAO
import com.mongodb.casbah.Imports._
import com.cloudwick.mongo.OptionsConfig
import org.slf4j.LoggerFactory

/**
 * Batch inserts events to mongo
 * @param eventsStartRange start range of the number of documents to insert
 * @param eventsEndRange end range of the number of documents to insert
 * @param counter atomic counter for keeping track of all threads insert operations
 * @param config scopt parsed command line options
 * @param mongo log event documents data access object
 * @author ashrith 
 */
class BatchInsert(eventsStartRange: Int,
                  eventsEndRange: Int,
                  counter: AtomicLong,
                  config: OptionsConfig,
                  mongo: LogDAO) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  lazy val retryBlock = new com.cloudwick.generator.utils.Retry[Unit](config.operationRetires)

  var batchMessagesCount: Int = 0
  val batch: ListBuffer[MongoDBObject] = new ListBuffer[MongoDBObject]

  val utils = new Utils
  val ipGenerator = new IPGenerator(config.ipSessionCount, config.ipSessionLength)
  val logEventGen = new LogGenerator(ipGenerator)

  val writeConcern = config.writeConcern match {
    case "none" => WriteConcern.None
    case "safe" => WriteConcern.Safe
    case "majority" => WriteConcern.Majority
  }

  def threadName = Thread.currentThread().getName

  def run() = {
    import retryBlock.retry
    val mongoClient = mongo.initialize
    val collection = mongo.initCollection(mongoClient, config.mongoDbName, config.mongoCollectionName)
    val totalDocs = eventsEndRange - eventsStartRange + 1
    try {
      (eventsStartRange to eventsEndRange).foreach { docCount =>
        batchMessagesCount += 1
        batch += mongo.makeMongoObject(logEventGen.eventGenerate, docCount)

        if (batchMessagesCount == config.batchSize || batchMessagesCount == totalDocs) {
          // logger.debug("Sending a batch to mongo for insert " + config.batchSize)
          retry {
            mongo.batchAdd(collection, batch, writeConcern)
          } giveup {
            case e: Exception =>
              logger.debug("failed inserting batch to mongo collection after {} tries, reason: {}",
                config.operationRetires, e.printStackTrace())
          }
          counter.getAndAdd(batchMessagesCount)
          batchMessagesCount = 0
          batch.clear()
        }
      }
      logger.debug(s"Documents inserted (with batchSize of ${config.batchSize}) by $threadName is: $totalDocs from" +
        s"($eventsStartRange) to ($eventsEndRange)")
      // counter.getAndAdd(totalDocs)
    } finally {
      mongo.close(mongoClient)
    }
  }
}
