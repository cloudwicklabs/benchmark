package com.cloudwick.solr.dao
import jp.sf.amateras.solr.scala.{Order, SolrClient}

/**
 * Searches solr index for specified query using solr-scala-client
 * @param solrURL solr server url to connect to
 * @author ashrith 
 */
class SearchLogs(solrURL:String) {
  val solrClient = new SolrClient(solrURL)

  /**
   * executes a given solr query
   * @param query to perform on the solr server
   * @return
   */
  def executeQuery(query: String, rowsCount: Int) = {
    solrClient.query(query)
      .rows(rowsCount)
      .fields("ip", "timestamp", "response", "bytes_sent", "request_page", "browser")
      .sortBy("timestamp", Order.asc)
      .getResultAsMap()
  }

  /**
   * executes a single document returning query
   * @param query to execute
   * @return
   */
  def querySingleDocument(query: String) = solrClient.query(query)

  /**
   * Calculates number of documents available
   * @return Long number of documents in the index
   */
  def getCount = solrClient.query("*:*").rows(0).getResultAsMap().numFound

  /**
   * pretty prints a solr document
   * @param document to pretty print
   */
  def prettyPrint(document: Map[String, Any]) = {
    println()
    document.foreach { doc =>
      println(s"\t${doc._1}: ${doc._2}")
    }
  }
}
