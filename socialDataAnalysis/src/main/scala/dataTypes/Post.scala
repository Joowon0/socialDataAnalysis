package dataTypes

/**
  * Class for handling posts and comments
  */

import libFromCoursera.Var

trait Writing {
  def getScore() : Int
}

case class Post(PostID: Long, timestamp: Timestamp) extends Writing {
  //def getScore() = timestamp.score
  val score = Var(10)
  def getScore() : Int = score()

  def decrease(): Unit = {
    val temp = score() - 1
    score() = temp
  }

  override def toString: String =
    "\nID : " + PostID + ", " +
    "TS : " + timestamp.toString +
    "sc : " + getScore() + "\n"
}

// date_timestamp  - only records date and scores
//                   used in Query1
// sec_timestamp   - records sec
//                   used in Query2
case class Comment (commentID: Long, date_timestamp: Timestamp) extends Writing {
  val score = Var(10)
  def getScore() : Int = score()

  def decrease(): Unit = {
    val temp = score() - 1
    score() = temp
  }
  override def toString: String =
    "\nID : " + commentID + ", " +
      "TS : " + date_timestamp +
      "SC : " + getScore()
}

case object Empty extends Writing {
  def getScore(): Int = 0
}

