package com.cloudwick.solr.dao

import scala.collection.mutable.ArrayBuffer
import org.apache.solr.client.solrj.impl.HttpSolrServer
import com.cloudwick.generator.log.LogEvent
import org.apache.solr.common.SolrInputDocument

/**
 * Indexes apache http log events to solr using solr library
 * @param serverURL solr server url to connect
 * @author ashrith 
 */
class IndexLogs(serverURL: String) {
  val server = new HttpSolrServer(serverURL)

  /**
   * Generates random uuid
   * @return
   */
  def uuid = java.util.UUID.randomUUID.toString

  /**
   * Wraps LogEvent into SolrDocument
   * @param logEvent logEvent object to wrap solr document with
   * @return
   */
  def getSolrDocument(logEvent:LogEvent, recordId: Int) = {
    val document = new SolrInputDocument()
    document.addField("id", uuid)
    document.addField("record_id", recordId)
    document.addField("ip", logEvent.ip)
    document.addField("timestamp", logEvent.timestamp)
    document.addField("request_page", logEvent.request)
    document.addField("response", logEvent.responseCode)
    document.addField("bytes_sent", logEvent.responseSize)
    document.addField("browser", logEvent.userAgent)
    document
  }

  /**
   * Deletes existing index
   * @return
   */
  def cleanup = {
    server.deleteByQuery("*:*")
    server.commit()
  }

  /**
   * Indexes events & commits them
   * @param logEvents list of logEvents to index
   * @return
   */
  def send(logEvents:ArrayBuffer[LogEvent]) = {
    logEvents.zipWithIndex.foreach{ case(logEvent, i) =>
      server.add(getSolrDocument(logEvent, i))
    }
    server.commit()
  }

  /**
   * Indexes single log event
   * @param logEvent single logEvent to index
   * @return
   */
  def indexDocument(logEvent: LogEvent, recordId: Int) = {
    server.add(getSolrDocument(logEvent, recordId))
  }

  /**
   * Commits documents to solr index
   * @return
   */
  def commitDocuments = {
    server.commit()
  }

}
