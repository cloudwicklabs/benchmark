package com.cloudwick.mongo

/**
 * Class for wrapping default command line options
 * @author ashrith 
 */
case class OptionsConfig(
  mode: String = "insert",
  mongoURL: String = "mongodb://localhost:27017/",
  eventsPerSec: Int = 1,
  totalEvents: Seq[Int] = Seq(),
  ipSessionCount: Int = 25,
  ipSessionLength: Int = 50,
  mongoDbName: String = "logs",
  mongoCollectionName: String = "logEvents",
  batchSize: Int = 0,
  indexData: Boolean = false,
  shardMode: Boolean = false,
  shardPreSplit: Boolean = false,
  writeConcern: String = "none",
  readPreference: String = "primary",
  operationRetires: Int = 10,
  threadsCount: Int = 1,
  threadPoolSize: Int = 10
)
