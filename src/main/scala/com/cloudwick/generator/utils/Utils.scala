package com.cloudwick.generator.utils

import scala.util.Random
import org.slf4j.LoggerFactory

/**
 * Set of utilities used in generating data
 * @author ashrith 
 */
class Utils {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Picks an element out of map, following the weight as the probability
   * @param map map of key value pairs having key to picks and its value as the probability
   *            of picking that key
   * @return randomly picked key selected from map
   */
  def pickWeightedKey(map: Map[String, Int]): String = {
    var total = 0
    map.values.foreach { weight => total += weight }
    val rand = Random.nextInt(total)
    var running = 0
    for((key, weight) <- map) {
      if(rand >= running && rand < (running + weight)) {
        return key
      }
      running += weight
    }
    map.keys.head
  }

  /**
   * Returns a random number between min and max, inclusive.
   * The difference between min and max can be at most `Integer.MAX_VALUE - 1`
   * @param min Minimum value
   * @param max Maximum value
   * @return Integer between min and max, inclusive
   */
  def randInt(min: Int, max: Int) = {
    Random.nextInt((max - min) + 1) + min
  }

  /**
   * Measures time took to run a block
   * @param block code block to run
   * @param message additional message to print
   * @tparam R type
   * @return returns block output
   */
  def time[R](message: String = "code block")(block: => R): R = {
    val s = System.nanoTime
    // block: => R , implies call by name i.e, the execution of block is delayed until its called by name
    val ret = block
    logger.info("Time elapsed in " + message + " : " +(System.nanoTime - s)/1e6+"ms")
    ret
  }
}