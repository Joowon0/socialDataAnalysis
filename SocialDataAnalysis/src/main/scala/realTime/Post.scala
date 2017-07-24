package realTime

import scala.collection.parallel.mutable.ParHashSet


// would be more efficient to make a class with 3 posts with order
trait Writing {
  val totalScore : Int
}

case class Post (timestamp: Timestamp) extends Writing {
  val totalScore = Var(0)
  def update() : Unit =  //problem here
    totalScore() = comments map (_.totalScore) sum

  def addComment(comment: Comment) : Unit =
    comments + comment

  // a set of connected comments
  val comments : ParHashSet[Comment] = ParHashSet()
}

case class Comment ( commentID: Long, timestamp: Timestamp) extends Writing {
  val totalScore = timestamp.score // this might be changed into Var()
}

case object Empty extends Writing {
  val totalScore = 0
}


class Timestamp {
  val timestamp = ???
  val score = Var(10)
}