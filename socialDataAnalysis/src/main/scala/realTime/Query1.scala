package realTime

/**
  * Classes for handling Query1
  */

import scala.collection.parallel.mutable._

class ThreePosts {
  val post1 : Var[Writing] = Var(Empty)
  val post2 : Var[Writing] = Var(Empty)
  val post3 : Var[Writing] = Var(Empty)

  // insert in sorted
  def insert(post : Post): Unit =
    if (post.getScore() >= post1().getScore()) {
      val p1 = post1()
      val p2 = post2()

      post1() = post
      post2() = p1
      post3() = p2
    }
    else if (post.getScore() >= post2().getScore()) {
      val p2 = post2()

      post3() = p2
      post2() = post
    }
    else if (post.getScore() >= post3().getScore()) {
      post3() = post
    }

  // get top 3 posts that do not have score 0
  def getTopPosts() : List[Post] =
    post1() match {
      case Empty      => List()
      case Post(_,_)  =>
        post2() match {
          case Empty     => List(post1().asInstanceOf)
          case Post(_,_) =>
            post3() match {
              case Empty     => List(post2().asInstanceOf, post1().asInstanceOf)
              case Post(_,_) => List(post3().asInstanceOf, post2().asInstanceOf, post1().asInstanceOf)
            }
        }
    }
}

object Query1 {
  // a value that contains recent top3
  val TOP3 = new ThreePosts
  // all of timestamps
  val daysTimestamp : ParHashSet[Timestamp] = ParHashSet()
  // a set of all post
  val posts : ParHashSet[Post] = ParHashSet()

  // given a comment ID, able to find corresponding posts
  val connectedPost : ParHashMap[Long, Post] = ParHashMap() // withDefault (_ => Empty)
  def insertConnection(commentID: Long, post: Post) : Unit =
    connectedPost + (commentID, post)

  // This part should be changed into parallel
  def findTop3(): List[Post] = TOP3.getTopPosts()
  def findTop3(posts: ThreePosts): List[Post] = posts.getTopPosts()

}