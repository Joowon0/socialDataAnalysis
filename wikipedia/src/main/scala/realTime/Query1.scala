package realTime

/**
  * Classes for handling Query1
  */

import scala.collection.parallel.mutable.ParHashMap

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
      case Empty => List()
      case Post(_)  =>
        post2() match {
          case Empty   => List(post1().asInstanceOf)
          case Post(_) =>
            post3() match {
              case Empty   => List(post2().asInstanceOf, post1().asInstanceOf)
              case Post(_) => List(post3().asInstanceOf, post2().asInstanceOf, post1().asInstanceOf)
            }
        }
    }
}

object Query1 {
  val TOP3 = new ThreePosts

  // given a comment ID, able to find corresponding posts
  val connectedPost : ParHashMap[Long, Post] = ParHashMap()
  def insertConnection(commentID: Long, post: Post) : Unit =
    connectedPost + (commentID, post)

  // This part should be changed into parallel
  def findTop3(): List[Post] = TOP3.getTopPosts()
  def findTop3(posts: ThreePosts): List[Post] = posts.getTopPosts()
}