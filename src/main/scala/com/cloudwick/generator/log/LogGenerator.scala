package com.cloudwick.generator.log

import scala.util.Random
import com.cloudwick.generator.utils.Utils

/**
 * Generates generator.log event
 * @constructor creates a new logGenerator
 * @author ashrith 
 */
class LogGenerator(var ipGenerator:IPGenerator) {
  val utils = new Utils

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

  def eventGenerate = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val date = format.format(new java.util.Date())
    new LogEvent(
      ipGenerator.get_ip,
      date,
      utils.pickWeightedKey(URLS),
      utils.pickWeightedKey(RESPONSE_CODES).toInt,
      Random.nextInt(2 * 1024) + 192,
      utils.pickWeightedKey(USER_AGENTS)
    )
  }
}
