package realTime

/**
  * Class for handling posts and comments
  */

import scala.collection.parallel.mutable.ParHashSet
import java.util.Date

trait Writing {
  def getScore() : Int
}

case class Post(timestamp: Timestamp) extends Writing {
  val totalScore: Var[Int] = Var(0)
  def getScore(): Int = totalScore()
  val comments : ParHashSet[Comment] = ParHashSet()

  def update(): Unit =
    totalScore() = (comments map (_.getScore()) sum) + timestamp.score()
  def addComment(comment: Comment) : Unit =
    comments + comment
}

case class Comment (commentID: Long, timestamp: Timestamp) extends Writing {
  def getScore() = timestamp.score()
}

case object Empty extends Writing {
  def getScore(): Int = 0
}

// use method Date(int year, int month, int date) for new Date
class Timestamp(timestamp: Date) {
  val score = Var(10)

  def decrease() : Unit = {
    val temp = score()
    score() = temp - 1
  }

  def isPast(today : Date): Boolean = {
    if(timestamp.compareTo(today) == -1)
  }
}