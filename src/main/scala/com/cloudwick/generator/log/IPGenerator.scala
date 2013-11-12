package com.cloudwick.generator.log

import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

/**
 * Generates random IP address based on the session information
 * @constructor create new ipGenerator with session count and session length
 * @param sessionCount number of ip(s) in the sessions buffer
 * @param sessionLength total length og the session
 * @author ashrith 
 */
class IPGenerator(var sessionCount: Int, var sessionLength: Int) {
  var sessions = Map[String, Int]()

  def get_ip = {
    sessionGc()
    sessionCreate()
    val ip = sessions.keys.toSeq(Random.nextInt(sessions.size))
    sessions(ip) += 1
    ip
  }

  private def sessionCreate() = {
    while(sessions.size < sessionCount) {
      sessions(randomIp) = 0
    }
  }

  private def sessionGc() = {
    for((ip, count) <- sessions) {
      if(count >= sessionLength)
        sessions.remove(ip)
    }
  }

  private def randomIp: String = {
    val random = Random
    var octets = ArrayBuffer[Int]()
    octets += random.nextInt(223) + 1
    (1 to 3).foreach {_ => octets += random.nextInt(255)}
    octets.mkString(".")
  }
}
