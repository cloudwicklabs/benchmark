package com.cloudwick.solr

/**
 * Case class for wrapping scopt options parser options
 * @author ashrith 
 */
case class OptionsConfig (
  mode:String = "index",
  solrServerUrl: String = "http://localhost:8983/solr/logs",
  totalEvents: Seq[Int] = Seq(),
  eventsPerSec: Int = 1,
  ipSessionCount: Int = 5,
  ipSessionLength: Int = 5,
  cleanPreviousIndex: Boolean = false,
  solrQuery:String = "*:*",
  batchSize: Int = 1000,
  queryCount:Int = 10
)
