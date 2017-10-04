package dataTypes

import scala.collection.parallel.mutable.ParHashMap
import scala.collection.parallel.mutable.ParHashSet

object Query2 {
  // a set of all comments
  val comments : ParHashSet[Comment] = ParHashSet()
  // given an id, show all friendships
  val friendship : ParHashMap[Long,ParHashSet[Long]] = ParHashMap()

  // make edges in in all comments
  def newFriendship(id1: Long, id2: Long) : Unit = {
    friendship(id1) += (id2)
    friendship(id2) += (id1)

    comments map { c =>
      if (c.ifVertex(id1) && (c.ifVertex(id2)))
        c.makeEdge(id1, id2)
    }
  }
}
