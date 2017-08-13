package realTime

import scala.collection.parallel.mutable.ParHashMap


class ThreePosts {
  val post1 : Writing = Empty
  val post2 : Writing = Empty
  val post3 : Writing = Empty

  // insert in sorted
  def insert(post : Post): Unit = ???

  // get top 3 posts that do not have score 0
  def getTopPosts() =
    post1 match {
      case Empty => List()
      case Post(_)  =>
        post2 match {
          case Empty   => List(post1)
          case Post(_) =>
            post3 match {
              case Empty   => List(post2, post1)
              case Post(_) => List(post3, post2, post1)
            }
      }
    }
}


object Query1 {
  val TOP3 = new ThreePosts

  // given a comment ID, able to find corresponding post
  val connectedPost : ParHashMap[Long, Post] = ParHashMap()

  def findTop3(posts: List[Post]): List[Post] = ???
}