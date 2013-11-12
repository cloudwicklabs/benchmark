package com.cloudwick.generator.movie

import scala.io.Source
import scala.util.Random
import com.cloudwick.generator.utils._
import scala.collection.mutable

/**
 * Mocks movie
 * @author ashrith 
 */
class MovieGenerator {
  val movieTitles = io.Source.fromURL(getClass.getResource("/movie_titles.csv"))
  val data = parseToMap(movieTitles.getLines())
  val random = Random
  val utils = new Utils

  val MOVIE_GENRE = Map(
    "action" -> 10,
    "comedy" -> 10,
    "family" -> 10,
    "history" -> 10,
    "adventure" -> 10,
    "horror" -> 10,
    "documentary" -> 10,
    "drama" -> 10,
    "romance" -> 10,
    "scifi" -> 10
  )

  /**
   * Builds a new map of titles of the form Map('movie_id', Map(year -> 'release_year', name -> "movie_title"))
   * @param movieTitlesFile Iterator of the file contents
   */
  def parseToMap(movieTitlesFile: Iterator[String]): mutable.Map[String, Map[String, String]] = {
    val map = mutable.Map[String, Map[String, String]]()
    for (line <- movieTitlesFile) {
      val id :: year :: name = line.split(',').toList
      map += id -> Map("year" -> year, "name" -> name.mkString)
    }
    map
  }

  def gen: Array[String] = {
    val mid = (random.nextInt(data.size) + 1).toString
    Array(mid, data(mid)("name"), data(mid)("year"), utils.randInt(50, 90).toString, utils.pickWeightedKey(MOVIE_GENRE))
  }
}
