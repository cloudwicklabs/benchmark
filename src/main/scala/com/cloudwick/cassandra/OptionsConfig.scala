package com.cloudwick.cassandra

/**
 * Wraps default command line options for cassandra driver
 * @author ashrith 
 */
case class OptionsConfig(
  mode: String = "insert",
  cassandraNode: Seq[String] = Seq("127.0.0.1"),
  totalEvents: Seq[Int] = Seq(),
  keyspaceName: String = "moviedata",
  customerDataSetSize: Int = 1000,
  batchSize: Int = 0,
  dropExistingTables: Boolean = false,
  replicationFactor: Int = 1,
  async: Boolean = false
)
