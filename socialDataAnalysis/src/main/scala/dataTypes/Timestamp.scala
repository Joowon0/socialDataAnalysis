package dataTypes

import java.util.Date
import libFromCoursera.Var

// use method Date(int year, int month, int date) for new Date
class Timestamp(timestamp: Date) extends java.io.Serializable {
  val score = Var(10)

  def decrease() : Unit = {
    val temp = score()
    score() = temp - 1
  }

  /*  def isPast(today : Date): Boolean =
      timestamp.compareTo(today) == -1*/

  override def toString: String = timestamp.toString + " : " + score() + ", "
}