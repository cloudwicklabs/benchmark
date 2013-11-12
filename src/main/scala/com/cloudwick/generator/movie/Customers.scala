package com.cloudwick.generator.movie

import com.cloudwick.generator.utils.Utils
import scala.util.Random
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Generates customer records
 * @author ashrith 
 */
class Customers(cId: Int, cName: String, movieTotalTime: Int) {
  val utils = new Utils
  val random = Random
  val formatter = new SimpleDateFormat("dd-MMM-yy HH:mm:ss")

  val ACTIVE_INACTIVE = Map(
    "0" -> 80,
    "1" -> 20
  )

  val RATINGS = Map(
    "-1"  -> 30,
    "5"   -> 5,
    "4.5" -> 15,
    "4"   -> 15,
    "3.5" -> 20,
    "3"   -> 5,
    "2.5" -> 5,
    "2"   -> 2,
    "1"   -> 2,
    "0"   -> 1
  )

  val PAUSED_TIME = Map(
    "0" -> 80,
    "1" -> 20
  )

  val custId = cId
  val custName = cName
  val movieTT = movieTotalTime
  val userActiveOrNot = userActive
  val timeWatched = genDate("01-Jan-10 12:10:00", formatter.format(Calendar.getInstance().getTimeInMillis))
  val pausedTime = playedTime
  val rating = genRating

  override def toString = s"$cId <=> $cName <=> $userActiveOrNot <=> $timeWatched <=> $pausedTime <=> $rating"

  private def genRating = utils.pickWeightedKey(RATINGS)

  private def userActive = utils.pickWeightedKey(ACTIVE_INACTIVE)

  /**
   * Generates random date between a given range of dates
   * @param from date range from which to generate the date from, should be of the format
   *             'dd-MMM-yy HH:mm:ss'
   * @param to date range end, should be of the format 'dd-MMM-yy HH:mm:ss'
   * @return a random date in milli seconds
   */
  private def genDate(from: String, to: String) = {
    val cal = Calendar.getInstance()
    cal.setTime(formatter.parse(from))
    val v1 = cal.getTimeInMillis
    cal.setTime(formatter.parse(to))
    val v2 = cal.getTimeInMillis

    val diff: Long = (v1 + Math.random() * (v2 - v1)).toLong
    cal.setTimeInMillis(diff)
    cal.getTimeInMillis
  }

  private def playedTime = {
    utils.pickWeightedKey(PAUSED_TIME) match {
      case "0" => 0
      case "1" => utils.randInt(1, movieTotalTime)
    }
  }
}
