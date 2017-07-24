package realTime

import scala.collection.parallel.mutable.ParHashMap

object Query1 {
  object TOP3 {
    val post1 : Post = ??? // this should be initiated into empty post
    val post2 : Post = ???
    val post3 : Post = ???
  }

  // given a comment ID, albe to find corresponding post
  val connectedPost : ParHashMap[Long, Post] = ParHashMap()

  def findTop3(posts: List[Post]): List[Post] = ???
}