package realTime

/**
  * A class for handling posts and comments
  */

import scala.collection.parallel.mutable.ParHashSet

trait Writing {
  def getScore() : Int
}

case class Post(timestamp: Timestamp) extends Writing {
  val totalScore: Var[Int] = Var(0)
  def getScore(): Int = totalScore()
  val comments : ParHashSet[Comment] = ParHashSet()

  def update(): Unit =
    totalScore() = comments map (_.getScore()) sum
  def addComment(comment: Comment) : Unit =
    comments + comment
}

case class Comment ( commentID: Long, timestamp: Timestamp) extends  Writing {
  def getScore() = timestamp.score()
}

case object Empty extends Writing {
  def getScore(): Int = 0
}

class Timestamp {
  val timestamp = ???
  val score = Var(10)
}