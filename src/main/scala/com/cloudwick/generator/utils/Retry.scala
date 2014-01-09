package com.cloudwick.generator.utils

import scala.math._
import scala.util.control.Exception._

/**
 * Retries a block of code given number of times
 *
 * {{{
 *   val retryBlock = new Retry[String](6)
 *   import retryBlock.retry
 *
 *   retry {
 *    doSomething(somePar)
 *   } giveup {
 *    case e: Exception => handleException(e)
 *   }
 * }}}
 * @author Ashrith
 */
class Retry[T](maxRetry: Int) {
  // from https://gist.github.com/realbot/983175
  private var retryCount = 0
  private var block: () => T = _

  def retry (op: => T): Retry[T] = {
    block = () => {
      try {
        op
      } catch {
        case t: Throwable =>
          retryCount += 1
          if (retryCount == maxRetry) {
            throw t
          } else {
            val interval = round(pow(2, retryCount)) * 100
            Thread.sleep(interval)
            block()
          }
      }
    }
    this
  }

  def giveup (handler: => Catcher[T]): T = {
    catching(handler) {
      block()
    }
  }
}
