package realTime

import libFromCoursera.parallel

object Threads {
  val threshold = 100

  /** posts that should be done in real-time **/
  def postRealTime(getToWork: Int): Unit = {
    // the number of posts in newPosts
    val newPostsNum = Query1.postsUpdate.count(_ => true)
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
        val postHandle = Query1.postsUpdate.dequeue()

        //Query1.posts += postHandle

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
}