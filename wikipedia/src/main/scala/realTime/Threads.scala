package realTime

import common._
import java.util.Date

object Threads {
  val threshold = 100

  /** posts that should be done in real-time **/
  def postRealTime(getToWork: Int): Unit = {
    // the number of posts in newPosts
    val newPostsNum = Queue.newPosts.count(_ => true)
    // number of works we need to do
    val loadNum =
      if (getToWork == 0) newPostsNum
      else if (getToWork > newPostsNum) newPostsNum
      else getToWork

    // a work of a thread
    if (loadNum < threshold) {
      val threePosts = new ThreePosts

      var i = loadNum

      while (i > 0) {
        val postHandle = Queue.newPosts.dequeue()

        Query1.posts += postHandle

        i = i - 1
      }

      threePosts.getTopPosts() map (Query1.TOP3 insert (_))
    }
    // dividing into parallel
    else {
      /** Not sure if this part works in parallel*/
      val (a1, a2) = parallel(postRealTime(loadNum / 2), postRealTime(loadNum - loadNum / 2))

    }
  }


  /** called when a day pass by **/
  def calculate(): Unit = { // not sure if this is it
    Query1.daysTimestamp map (_.decrease())

    // these numbers should be changed into variables
    Query1.daysTimestamp += new Timestamp(new Date(2010, 3, 1))

  }
}