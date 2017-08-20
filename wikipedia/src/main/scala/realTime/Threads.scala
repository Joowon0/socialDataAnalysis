package realTime

import scala.collection.mutable
import common._

object Queue {
  val newPosts = new mutable.Queue
  val newComment = new mutable.Queue
}

object Threads {
  val threshold = 100

  // things that should be done in real-time
  def realTime(getToWork: Int): List[Post] = {
    // number of works we need to do
    val loadNum =
      if (getToWork == 0) Queue.newPosts.count(_ => true)
      else getToWork

    if (loadNum < threshold) {
      val threePosts = new ThreePosts

      

      threePosts.getTopPosts()
    } else {
      val (a1, a2) = parallel(realTime(loadNum / 2), realTime(loadNum - loadNum / 2))
      val threePosts = new ThreePosts

      a1 map (threePosts.insert(_))
      a2 map (threePosts.insert(_))

      threePosts.getTopPosts()
    }
  }

  // called when a day pass by
  def calculate(): Unit = {

  }
}
