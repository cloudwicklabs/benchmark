package com.cloudwick.mongo.dao

import com.mongodb.casbah.Imports._
import com.cloudwick.generator.log.LogEvent
import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory

/**
 * Interface for mongo
 * @author ashrith 
 */
class LogDAO(mongoConnectionUrl: String) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Initializes db connection to mongo & creates db and collection if does not exist
   * @return MongoCollection object
   */
  def initialize: MongoClient = {
    val mongoClientUri = MongoClientURI(mongoConnectionUrl)
    MongoClient(mongoClientUri)
  }

  /**
   * Terminates db connection
   * @param mongoClient mongo client object to terminate
   */
  def close(mongoClient: MongoClient) = {
    mongoClient.close()
  }

  /**
   * Connects|Creates db and collection specified
   * @param mongoClient mongo client object to connect to
   * @param database name of the db to connect to (creates if does not exist)
   * @param collection name of the collection to use (creates if does not exist)
   * @return MongoCollection object
   */
  def initCollection(mongoClient: MongoClient, database: String, collection: String): MongoCollection = {
    mongoClient(database)(collection)
  }

  /**
   * Creates shard collection on hashed key 'record_id'
   * @param mongoClient mongo client object to connect to
   * @param database name of the db to create
   * @param collection name of the collection to use
   * @return MongoCollection object
   */
  def initShardCollection(mongoClient: MongoClient, database: String, collection: String) = {
    val adminDB = mongoClient("admin")
    adminDB.command(MongoDBObject("enableSharding" -> database))
    val shardKey = MongoDBObject("record_id" -> "hashed")
    mongoClient(database)(collection).ensureIndex(shardKey)
    adminDB.command(MongoDBObject("shardCollection" -> s"${database}.${collection}", "key" -> shardKey))
  }

  /**
   * Drops a collection
   * @param collection mongo collection object to drop
   */
  def dropCollection(collection: MongoCollection) = {
    collection.drop()
  }

  /**
   * Drops a database, use this with caution removes all data on disk
   * @param database mongo database object to drop
   */
  def dropDatabase(database: MongoDB) = {
    database.dropDatabase()
  }

  /**
   * Creates indexes on top of existing collection
   * @param collection mongo collection to index on
   * @param indexKeys list of keys to index the collection on
   * @return None
   */
  def createIndexes(collection: MongoCollection, indexKeys: List[String]) = {
    logger.info("Creating indexes on :" + indexKeys.mkString(", "))
    indexKeys.map{ key =>
      collection.ensureIndex(MongoDBObject(key.toString -> 1))
    }
  }

  /**
   * Batch inserts documents into mongo
   * @param collection mongo collection object to insert documents to
   * @param documents list of documents to insert in batches
   * @return None
   */
  def batchAdd(collection: MongoCollection, documents: ListBuffer[MongoDBObject], concernLevel: WriteConcern) = {
    // :_* will get the elements instead of the list and avoid creating a List of List
    collection.setWriteConcern(concernLevel)
    collection.insert(documents:_*)
  }

  /**
   * Inserts a single document to mongo with specified write concern level
   * @param collection mongo collection object to insert document to
   * @param doc mongo document object to insert
   * @return WriteResult
   */
  def addDocument(collection: MongoCollection, doc: MongoDBObject, concernLevel: WriteConcern): WriteResult = {
    collection.insert(doc, concernLevel)
  }

  /**
   * Wraps a LogEvent object to MongoDBObject
   * @param logEvent logEvent to warp as MongoDBObject
   * @return
   */
  def makeMongoObject(logEvent: LogEvent, recordId: Int): MongoDBObject = {
    MongoDBObject(
      "record_id" -> recordId,
      "ip" -> logEvent.ip,
      "timestamp" -> logEvent.timestamp,
      "request_page" -> logEvent.request,
      "response_code" -> logEvent.responseCode,
      "response_bytes" -> logEvent.responseSize,
      "user_agent" -> logEvent.userAgent
    )
  }

  /**
   * Query mongo collection using a mongoDBObject
   * @param collection mongo collection object to query on
   * @param query query document object
   * @return
   */
  def findDocuments(collection: MongoCollection, query: MongoDBObject) = {
    collection.find(query)
  }

  /**
   * Query mongo collection using a mongoDBObject and returns single document
   * @param collection mongo collection object to query on
   * @param query query document object
   * @return
   */
  def findDocument(collection: MongoCollection, query: MongoDBObject) = {
    collection.findOne(query)
  }

  /**
   * Finds out the size of a given collection
   * @param collection object to count
   * @return
   */
  def documentsCount(collection: MongoCollection): Int = {
    collection.count().toInt
  }

  /**
   * Sets the read preference for a given collection
   * @param collection to set the read preference for
   * @param preference type of preference to set (possible values: primary, primaryPreferred, secondary,
   *                   secondaryPreferred, nearest)
   */
  def setReadPreference(collection: MongoCollection, preference: String) = {
    val readPref: ReadPreference = preference match {
      case "primary" => ReadPreference.Primary
      case "primaryPreferred" => ReadPreference.primaryPreferred
      case "secondary" => ReadPreference.Secondary
      case "secondaryPreferred" => ReadPreference.SecondaryPreferred
      case "nearest" => ReadPreference.Nearest
    }
    collection.setReadPreference(readPref)
  }

  /**
   * Performs aggregation queries on mongo and prints the result to the stdout
   *
   * === Usage ===
   * To perform 'grouping'
   *
   * val groupStmt =  MongoDBObject("$group" -> MongoDBObject(
   *    "_id" -> MongoDBObject("response_code" -> "$response_code"),
   *    "status_count" -> MongoDBObject("$sum" -> 1)
   * ))
   * val pipeLine = MongoDBList(groupStatement)
   *
   * aggregationResult(mongoClient, "logs", "logEvents", pipeline) match {
   *   case list: BasicDBList => list.foreach(println(_))
   *   case _ => println("didn't work")
   * }
   *
   * @param mongoClient mongo client object to use
   * @param database database to use
   * @param collectionName name of the collection to perform aggregation on
   * @param pipeline MongoDBList object of aggregation steps
   * @return None
   */
  def aggregationResult(mongoClient: MongoClient, database: String, collectionName:String, pipeline: MongoDBList) = {
    val db = mongoClient(database)
    db.command(MongoDBObject("aggregate" -> collectionName, "pipeline" -> pipeline)).get("result") match {
      case list: BasicDBList => {
        println("==================================")
        list.foreach(println)
        println("==================================")
      }
      case _ => println("Somthing went wrong executing the query " + pipeline.toString())
    }
  }

  /**
   * Builds aggregate query statement to find grouped by count of status codes
   * @return a MongoDBList object that could be passed to aggregationResult method which will in-turn execute
   *         the query on Mongo and prints the result
   */
  def buildQueryOne: MongoDBList = {
    val groupStmt = MongoDBObject("$group" -> MongoDBObject(
      "_id" -> MongoDBObject("responseCode" -> "$response_code"),
      "status_count" -> MongoDBObject("$sum" -> 1)
    ))
    MongoDBList(groupStmt)
  }

  /**
   * Builds aggregate query statement to co-relate request page to response code
   * @return a MongoDBList object
   */
  def buildQueryTwo: MongoDBList = {
    val groupStmt = MongoDBObject("$group" -> MongoDBObject(
      "_id" -> MongoDBObject("response" -> "$response_code", "access_page" -> "$request_page"),
      "status_count" -> MongoDBObject("$sum" -> 1)
    ))
    val sortStmt = MongoDBObject("$sort" -> MongoDBObject("_id.access_page" -> 1, "_id.response" -> 1))
    MongoDBList(groupStmt, sortStmt)
  }

  /**
   * Builds an aggregate query statement to count total number of bytes served for each page by web server
   * @return a MongoDBList object
   */
  def buildQueryThree: MongoDBList = {
    val groupStmt = MongoDBObject("$group" -> MongoDBObject(
      "_id" -> MongoDBObject("access_page" -> "$request_page"),
      "size" -> MongoDBObject("$sum" -> "$response_bytes")
    ))
    val sortStmt = MongoDBObject("$sort" -> MongoDBObject("_id.access_page" -> 1))
    MongoDBList(groupStmt, sortStmt)
  }

  /**
   * Builds an aggregate query statement to count how many times a client visited a particular page
   * @return a MongoDBList object
   */
  def buildQueryFour: MongoDBList = {
    val groupStmt = MongoDBObject("$group" -> MongoDBObject(
      "_id" -> "$ip",
      "size" -> MongoDBObject("$sum" -> 1)
    ))
    val projectStmt = MongoDBObject("$project" -> MongoDBObject(
      "_id" -> 0, "requester" -> "$_id", "visit_count" -> "$size")
    )
    MongoDBList(groupStmt, projectStmt)
  }

  /**
   * Builds an aggregate query statement to show top 10 visitors to the site
   * @return a MongoDBList object
   */
  def buildQueryFive: MongoDBList = {
    val groupStmt = MongoDBObject("$group" -> MongoDBObject(
      "_id" -> "$ip",
      "size" -> MongoDBObject("$sum" -> 1)
    ))
    val projectStmt = MongoDBObject("$project" -> MongoDBObject(
      "_id" -> 0, "requester" -> "$_id", "visit_count" -> "$size")
    )
    val sortStmt = MongoDBObject("$sort" -> MongoDBObject("visit_count" -> -1))
    val limitStmt = MongoDBObject("$limit" -> 10)
    MongoDBList(groupStmt, projectStmt, sortStmt, limitStmt)
  }

  /**
   * Builds an aggregate query statement to show top browsers
   * @return
   */
  def buildQuerySix: MongoDBList = {
    val groupStmt = MongoDBObject("$group" -> MongoDBObject(
      "_id" -> MongoDBObject("browser" -> "$user_agent"),
      "status_count" -> MongoDBObject("$sum" -> 1)
    ))
    MongoDBList(groupStmt)
  }
}