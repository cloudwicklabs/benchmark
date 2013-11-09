package com.cloudwick.generator.log

import scala.util.Random

/**
 * Generates generator.log event
 * @constructor creates a new logGenerator
 * @author ashrith 
 */
class LogGenerator(var ipGenerator:IPGenerator) {

  val RESPONSE_CODES = Map(
    "200" -> 92,
    "404" -> 5,
    "503" -> 3
  )

  val URLS = Map(
    "/" -> 50,
    "/aboutus.html" -> 5,
    "/contactus.html" -> 5,
    "/services.html" -> 15,
    "/services/nested_page.html" -> 5,
    "/test.php" -> 5,
    "/test.png" -> 5,
    "/test.gif" -> 5,
    "/test.css" -> 5
  )

  val USER_AGENTS = Map(
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)" -> 30,
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.50 Safari/534.24" -> 30,
    "Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1" -> 40
  )

  val random = Random

  private def pickWeightedKey(map: Map[String, Int]): String = {
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

  def eventGenerate = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val date = format.format(new java.util.Date())
    new LogEvent(
      ipGenerator.get_ip,
      date,
      pickWeightedKey(URLS),
      pickWeightedKey(RESPONSE_CODES).toInt,
      random.nextInt(2 * 1024) + 192,
      pickWeightedKey(USER_AGENTS)
    )
  }
}
