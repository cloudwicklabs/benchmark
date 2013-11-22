package com.cloudwick.cassandra.reads

import org.slf4j.LoggerFactory
import com.cloudwick.generator.utils.Utils
import com.cloudwick.generator.movie.MovieGenerator
import scala.util.Random
import com.cloudwick.cassandra.OptionsConfig
import scala.collection.JavaConversions._
import java.util.concurrent.atomic.AtomicLong

/**
 * Perform random reads on cassandra using pre-built random queries
 * @author ashrith 
 */
class Reads(numOfReads: Long,
            querySet: Map[String, String],
            counter: AtomicLong,
            customerDataSetSize: Int,
            config: OptionsConfig) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  val utils = new Utils
  val movie = new MovieGenerator
  val movieDAO = new com.cloudwick.cassandra.dao.MovieDAO(config.cassandraNode)

  def threadName = Thread.currentThread().getName

  def run() = {
    utils.time(s"reading $numOfReads by thread $threadName") {
      val movieInfo: Array[String] = movie.gen
      try {
        (1L to numOfReads).foreach { readQueryCount =>
          val randomQuery = s"query${Random.nextInt(4)+1}"
          val movieId = movieInfo(0)
          val movieReleaseYear = movieInfo(2)
          val movieGenre = movieInfo(4)
          val query: String = randomQuery match {
            case "query1" => querySet.get(randomQuery).get
              .replaceAll("CUSTID", (Random.nextInt(customerDataSetSize)+ 1).toString)
            case "query2" => querySet.get(randomQuery).get
              .replaceAll("CUSTID", (Random.nextInt(customerDataSetSize)+ 1).toString)
            case "query3" => querySet.get(randomQuery).get
              .replaceAll("CUSTID", (Random.nextInt(customerDataSetSize)+ 1).toString)
            case "query4" => querySet.get(randomQuery).get
              .replaceAll("GENRE", movieGenre)
              .replaceAll("RYEAR", movieReleaseYear)
              .replaceAll("MID", movieId)
          }
          try {
            movieDAO.findCQLByQuery(query)
          } catch {
            case ex: Exception => logger.warn(s"Failed executing query: '$query' reason: $ex")
          }
        }
        counter.getAndAdd(numOfReads)
      } finally {
          movieDAO.close()
      }
    }
  }
}
