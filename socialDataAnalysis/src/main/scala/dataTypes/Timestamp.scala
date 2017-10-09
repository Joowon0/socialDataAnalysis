package dataTypes

import java.util.Date
//import libFromCoursera.Var

// use method Date(int year, int month, int date) for new Date
class Timestamp(timestamp: Date) extends java.io.Serializable {
  /*var score = 10

  def decrease() : Unit = {
    score = score - 1
  }*/
  var score = 10

  def decrease() : Unit = {
    score = score - 1
  }

  /*  def isPast(today : Date): Boolean =
      timestamp.compareTo(today) == -1*/

  override def toString: String = timestamp.toString + " : " + score + ", "
}