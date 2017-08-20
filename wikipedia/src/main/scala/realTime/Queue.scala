package realTime

import scala.collection.mutable

object Queue {
  // posts that needs update
  val newPosts   = new mutable.Queue[Post]
  // comments that need to be added
  val newComment = new mutable.Queue[Comment]
}
