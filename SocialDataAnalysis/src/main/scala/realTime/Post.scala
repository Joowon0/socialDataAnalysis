package realTime

import scala.collection.parallel.mutable.ParHashSet

-time

// this should be splitted into empty and nonempty posts
// would be more efficient to make a class with 3 posts with order
class Post(timestamp: Timestamp) {
  val totalScore = Var(0)

  def update : Unit = ???

  // a set of connected comments
  val comments : ParHashSet[Long] = ParHashSet()
}