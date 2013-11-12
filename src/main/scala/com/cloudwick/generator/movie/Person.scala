package com.cloudwick.generator.movie

import scala.util.Random
import com.cloudwick.generator.utils.Utils

/**
 * Generates users names
 * @author ashrith 
 */
class Person {
  val random = Random
  val utils = new Utils

  val GENDERS = Map(
    "male" -> 56,
    "female" -> 44
  )

  val lastNames = Array(
    "ABEL", "ANDERSON", "ANDREWS", "ANTHONY", "BAKER", "BROWN", "BURROWS", "CLARK", "CLARKE", "CLARKSON", "DAVIDSON",
    "DAVIES", "DAVIS", "DENT", "EDWARDS", "GARCIA", "GRANT", "HALL", "HARRIS", "HARRISON", "JACKSON", "JEFFRIES",
    "JEFFERSON", "JOHNSON", "JONES", "KIRBY", "KIRK", "LAKE", "LEE", "LEWIS", "MARTIN", "MARTINEZ", "MAJOR", "MILLER",
    "MOORE", "OATES", "PETERS", "PETERSON", "ROBERTSON", "ROBINSON", "RODRIGUEZ", "SMITH", "SMYTHE", "STEVENS",
    "TAYLOR", "THATCHER", "THOMAS", "THOMPSON", "WALKER", "WASHINGTON", "WHITE", "WILLIAMS", "WILSON", "YORKE"
  )

  val maleFirstNames = Array(
    "ADAM", "ANTHONY", "ARTHUR", "BRIAN", "CHARLES", "CHRISTOPHER", "DANIEL", "DAVID", "DONALD", "EDGAR", "EDWARD",
    "EDWIN", "GEORGE", "HAROLD", "HERBERT", "HUGH", "JAMES", "JASON", "JOHN", "JOSEPH", "KENNETH", "KEVIN", "MARCUS",
    "MARK", "MATTHEW", "MICHAEL", "PAUL", "PHILIP", "RICHARD", "ROBERT", "ROGER", "RONALD", "SIMON", "STEVEN", "TERRY",
    "THOMAS", "WILLIAM"
  )

  val femaleFirstNames = Array(
    "ALISON", "ANN", "ANNA", "ANNE", "BARBARA", "BETTY", "BERYL", "CAROL", "CHARLOTTE", "CHERYL", "DEBORAH", "DIANA",
    "DONNA", "DOROTHY", "ELIZABETH", "EVE", "FELICITY", "FIONA", "HELEN", "HELENA", "JENNIFER", "JESSICA", "JUDITH",
    "KAREN", "KIMBERLY", "LAURA", "LINDA", "LISA", "LUCY", "MARGARET", "MARIA", "MARY", "MICHELLE", "NANCY", "PATRICIA",
    "POLLY", "ROBYN", "RUTH", "SANDRA", "SARAH", "SHARON", "SUSAN", "TABITHA", "URSULA", "VICTORIA", "WENDY"
  )

  val lettersArr = ('A' to 'Z').toList

  /**
   * Generates a random male or female name based on the probability
   * @return a name
   */
  def gen: String = {
    utils.pickWeightedKey(GENDERS) match {
      case "male" => maleName
      case "female" => femaleName
    }
  }

  private def initial = lettersArr(random.nextInt(lettersArr.size))

  private def lastName = lastNames(random.nextInt(lastNames.size))

  private def femaleName = s"${femaleFirstNames(random.nextInt(femaleFirstNames.size))} $initial $lastName"

  private def maleName = s"${maleFirstNames(random.nextInt(maleFirstNames.size))} $initial $lastName"
}
