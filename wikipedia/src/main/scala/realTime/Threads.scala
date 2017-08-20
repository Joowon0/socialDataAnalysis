package realTime

import common._
import java.util.Date

object Threads {
  val threshold = 100

  /** posts that should be done in real-time **/
  def postRealTime(getToWork: Int): List[Post] = {
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

      threePosts.getTopPosts()
    }
    // dividing into parallel
    else {
      val (a1, a2) = parallel(postRealTime(loadNum / 2), postRealTime(loadNum - loadNum / 2))
      val threePosts = new ThreePosts

      a1 map (threePosts.insert(_))
      a2 map (threePosts.insert(_))

      threePosts.getTopPosts()
    }
  }


  /** called when a day pass by **/
  def calculate(): Unit = { // not sure if this is it
    Query1.daysTimestamp map (_.decrease())

    // these numbers should be changed into variables
    Query1.daysTimestamp += new Timestamp(new Date(2017, 3, 1))

  }
}