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
  lazy val retryBlock = new com.cloudwick.generator.utils.Retry[Unit](config.operationRetires)
  val utils = new Utils
  val movie = new MovieGenerator

  def threadName = Thread.currentThread().getName

  def run() = {
    import retryBlock.retry
    val totalRecords = eventsEndRange - eventsStartRange + 1
    val movieDAO = new MovieDAO(config.cassandraNode)
    val customersSize = customersMap.size
    try {
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

        /*
        var operationTry = 0
        var success = false
        while (!success && operationTry < config.operationRetires) {
          try {
            movieDAO.loadWatchHistory(config.keyspaceName,
              customerID,
              customerTimeWatchedInit.toString,
              customerPausedTime,
              movieId.toInt,
              movieName,
              config.aSync)
            success = true
          } catch {
            case e: Exception => //ignore
          } finally {
            operationTry += 1
            Thread.sleep(500)
          }
        }
        */
        retry {
          movieDAO.loadWatchHistory(config.keyspaceName,
            customerID,
            customerTimeWatchedInit.toString,
            customerPausedTime,
            movieId.toInt,
            movieName,
            config.aSync)
        } giveup {
          case e: Exception =>
            logger.debug("failed inserting to 'WatchHistory' after {} tries, reason: {}",
              config.operationRetires, e.printStackTrace())
        }
        retry {
          movieDAO.loadCustomerRatings(config.keyspaceName,
            customerID,
            movieId.toInt,
            movieName,
            customerName,
            customerRating.toFloat,
            config.aSync)
        } giveup {
          case e: Exception =>
            logger.debug("failed inserting to 'CustomersRating' after {} tries, reason: {}",
              config.operationRetires, e.printStackTrace())
        }
        retry {
          movieDAO.loadCustomerQueue(config.keyspaceName,
            customerID,
            customerTimeWatchedInit.toString,
            customerName,
            movieId.toInt,
            movieName,
            config.aSync)
        } giveup {
          case e: Exception =>
            logger.debug("failed inserting to 'CustomerQueue' after {} tries, reason: {}",
              config.operationRetires, e.printStackTrace())
        }
        retry {
          movieDAO.loadMovieGenre(config.keyspaceName,
            movieGenre,
            movieReleaseYear.toInt,
            movieId.toInt,
            movieRunTime.toInt,
            movieName,
            config.aSync)
        } giveup {
          case e: Exception =>
            logger.debug("failed inserting to 'MovieGenre' after {} tries, reason: {}",
              config.operationRetires, e.printStackTrace())
        }
        counter.getAndAdd(4)
      }
      logger.debug(s"Records inserted by ${threadName} is : ${totalRecords * 4} from(${eventsStartRange}) to(${eventsEndRange})")
    } finally {
      movieDAO.close()
    }
  }
}
