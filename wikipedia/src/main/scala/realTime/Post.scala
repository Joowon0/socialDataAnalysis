package realTime

/**
  * Class for handling posts and comments
  */

import scala.collection.parallel.mutable.{ParHashMap, ParHashSet}
import java.util.Date

import scala.collection.parallel.immutable.ParSet

trait Writing {
  def getScore() : Int
}

case class Post(PostID: Long, timestamp: Timestamp) extends Writing {
  val totalScore: Var[Int] = Var(0)
  def getScore(): Int = totalScore()
  val comments : ParHashSet[Comment] = ParHashSet()

  def update(): Unit =
    totalScore() = (comments map (_.getScore()) sum) + timestamp.score()
  def addComment(comment: Comment) : Unit =
    comments + comment
}

// date_timestamp  - only records date and scores
//                   used in Query1
// sec_timestamp   - records sec
//                   used in Query2
case class Comment (commentID: Long, date_timestamp: Timestamp, sec_timestamp: Date) extends Writing {
  // Query1
  def getScore() = date_timestamp.score()


  // Query2
  // a graph for communities
  val commuGraph : ParHashMap[Long, ParHashSet[Long]] = ParHashMap((commentID, ParHashSet()))
  // check if the user pressed 'like'
  def ifVertex(id: Long) : Boolean = commuGraph contains (id)

  // called when there is a new 'like'
  def makeVertex(id: Long) = {
    commuGraph += ((id, ParHashSet()))

    val friends = Query2.friendship(id)
    friends filter ifVertex map {makeEdge(id,_)}
  }
  def makeEdge(id1: Long, id2: Long) = {
    commuGraph(id1)+=(id2)
    commuGraph(id2)+=(id1)
  }
  // get the biggest size of a community
  def getSize() : Int = {
    // given an id, find all friends of the id
    def getFriend(id : Long) : ParSet[Long] = {
      // merge origin set and friends set of base
      def mergeFriend(base : Long, origin : ParSet[Long]) : ParSet[Long] = {
        val friends : ParSet[Long] = commuGraph(base).toSet[Long]
        val getmore = friends flatMap {id =>
          if (id > base && !friends.contains(id))
            mergeFriend(id, (origin + base))
          else
            Set()
        }
        friends ++ getmore
      }
      val firstSet : ParSet[Long] = commuGraph(id).toSet[Long]
      firstSet flatMap {mergeFriend(_, firstSet)} // this part could be changed into more efficient way
    }
    def getAllSet(ids: List[Long]) : List[ParSet[Long]] =
      if (ids.isEmpty) List()
      else {
        val firstGroup = getFriend(ids.head)
        val restIDs = ids.tail filter {!firstGroup.contains(_)}
        firstGroup :: getAllSet(restIDs)
      }

    val allIDs = commuGraph.keys.toList
    getAllSet(allIDs) map (_.size) max
  }
}

case object Empty extends Writing {
  def getScore(): Int = 0
}

// use method Date(int year, int month, int date) for new Date
class Timestamp(timestamp: Date) {
  val score = Var(10)

  def decrease() : Unit = {
    val temp = score()
    score() = temp - 1
  }
  
  def isPast(today : Date): Boolean =
    timestamp.compareTo(today) == -1
}