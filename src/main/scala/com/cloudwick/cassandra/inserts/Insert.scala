package com.cloudwick.cassandra.inserts

import org.slf4j.LoggerFactory
import com.cloudwick.generator.utils.Utils
import com.cloudwick.cassandra.OptionsConfig
import java.util.concurrent.atomic.AtomicLong
import com.cloudwick.cassandra.dao.MovieDAO
import scala.collection.JavaConversions._
import scala.util.Random
import com.cloudwick.generator.movie.{MovieGenerator, Customers}

/**
 * Inserts events into cassandra tables
 * @author ashrith 
 */
class Insert(eventsStartRange: Int,
             eventsEndRange: Int,
             counter: AtomicLong,
             customersMap: Map[Int, String],
             config: OptionsConfig) extends Runnable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  val utils = new Utils
  val movie = new MovieGenerator

  def threadName = Thread.currentThread().getName

  def run() = {
    val totalRecords = eventsEndRange - eventsStartRange + 1
    val movieDAO = new MovieDAO(config.cassandraNode)
    val customersSize = customersMap.size
    try {
      utils.time(s"inserting $totalRecords by thread $threadName") {
        (eventsStartRange to eventsEndRange).foreach { eventCount =>
          val cID: Int = Random.nextInt(customersSize) + 1
          val cName: String = customersMap(cID)
          val movieInfo: Array[String] = movie.gen
          val customer: Customers = new Customers(cID, cName, movieInfo(3).toInt)
          val customerID: Int               = customer.custId
          val customerName: String          = customer.custName
          // val customerActive: String     = customer.userActiveOrNot
          val customerTimeWatchedInit: Long = customer.timeWatched
          val customerPausedTime: Int       = customer.pausedTime
          val customerRating: String        = customer.rating
          val movieId: String               = movieInfo(0)
          val movieName: String             = movieInfo(1).replace("'", "")
          val movieReleaseYear: String      = movieInfo(2)
          val movieRunTime: String          = movieInfo(3)
          val movieGenre: String            = movieInfo(4)

          movieDAO.loadWatchHistory(config.keyspaceName,
            customerID,
            customerTimeWatchedInit.toString,
            customerPausedTime,
            movieId.toInt,
            movieName,
            config.aSync)
          movieDAO.loadCustomerRatings(config.keyspaceName,
            customerID,
            movieId.toInt,
            movieName,
            customerName,
            customerRating.toFloat,
            config.aSync)
          movieDAO.loadCustomerQueue(config.keyspaceName,
            customerID,
            customerTimeWatchedInit.toString,
            customerName,
            movieId.toInt,
            movieName,
            config.aSync)
          movieDAO.loadMovieGenre(config.keyspaceName,
            movieGenre,
            movieReleaseYear.toInt,
            movieId.toInt,
            movieRunTime.toInt,
            movieName,
            config.aSync)
        }
        logger.info(s"Records inserted by $threadName is : ${totalRecords * 4} from($eventsStartRange) to($eventsEndRange)")
      }
      counter.getAndAdd(totalRecords)
    } finally {
      movieDAO.close()
    }
  }
}
