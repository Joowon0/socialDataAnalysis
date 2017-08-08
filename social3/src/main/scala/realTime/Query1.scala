package realTime

/**
  * A classes for handling Query1
  */

import scala.collection.parallel.mutable.ParHashMap

class ThreePosts {
  val post1 : Var[Writing] = Var(Empty)
  val post2 : Var[Writing] = Var(Empty)
  val post3 : Var[Writing] = Var(Empty)

  // insert in sorted
  def insert(post : Post): Unit = ???

  // get top 3 posts that do not have score 0
  def getTopPosts() : List[Writing] =
    post1() match {
      case Empty => List()
      case Post(_)  =>
        post2() match {
          case Empty   => List(post1())
          case Post(_) =>
            post3() match {
              case Empty   => List(post2(), post1())
              case Post(_) => List(post3(), post2(), post1())
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