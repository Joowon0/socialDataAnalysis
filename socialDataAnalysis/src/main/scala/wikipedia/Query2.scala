package wikipedia

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.{CommentInfo, FriendshipInfo, LikeInfo, PostInfo}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object Query2 {
  //val conf: SparkConf = new SparkConf().setMaster("spark://192.168.0.195:7077").setAppName("twitterAnalysis")
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis")
  val sc: SparkContext = new SparkContext(conf)

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }

  def main(args: Array[String]) {
    val df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse("2010-01-01T03:00:00.000+0000") // 12시 정오임

    //2010-02-01T12:00:00.000
    var currentDate : Timestamp = new Timestamp(date.getTime()) // java.util.Date

    var i = 0
    /** Don't know why start at Jan 01 */
    while (i < 29) {
      i = i + 1
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24)
      currentDate = new Timestamp(date.getTime())
    }
    val date2: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 36)
    currentDate = new Timestamp(date2.getTime())
    //new dataTypes.Timestamp(date2) +=: daysTimestamp // realTime.timestamp defined here

    /** calling recursive function */
    main_recur(1, 6 * 60 * 60, 5, sc.emptyRDD, List(new dataTypes.Timestamp(date2)))

    /**
      * @param i               - count the number of recursion
      * @param k               - number of post we want to get
      * @param d               - within k seconds
      * @param Friendships     - User ID  -> Set of user id
      * @param daysTimestamp   - all of timestamps
      */
    def main_recur(i : Int, k: Int, d: Int, Friendships : RDD[(Long, Long)], daysTimestamp : List[dataTypes.Timestamp] ) {
      if (i > 31) return

      /** RDD read from file */
      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/data_day/comments/comments" + i + ".dat").map(CommentsData.parse)
      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/data_day/friendships/friendships" + i + ".dat").map(FriendshipsData.parse)
      val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/data_day/likes/likes" + i + ".dat").map(LikesData.parse)
//      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments" + i + ".dat").map(CommentsData.parse)
//      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships" + i + ".dat").map(FriendshipsData.parse)
//      val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes" + i + ".dat").map(LikesData.parse)

      // print test
      println("현재 날짜    : " + currentDate.toString)
      val printTemp: String = (daysTimestamp map (x => x.toString)).mkString(" ")
      println("timestamps : " + printTemp)

      /** Comments */
      val newCommentsPair : RDD[(Long, Long)] = CommentsRDD.map(c=> (c.comment_id, c.comment_id))

      /** Likes */
      val likesCommentUser : RDD[(Long, Long)] = LikesRDD.map(c => (c.comment_id, c.user_id))
      val likesCommentUserGroup : RDD[(Long, Iterable[Long])] = likesCommentUser groupByKey
      val likesCommentUserNil : RDD[(Long, Iterable[Long])] =
        ((newCommentsPair leftOuterJoin likesCommentUserGroup) values) mapValues {
          case None => Iterable()
          case Some(x) => x
        }

      /** users that liked to new comments */
      val allUsers : RDD[Long] = likesCommentUser map (_._2) distinct
      val allUsersPair : RDD[(Long, Long)] = allUsers map (u => (u,u))

      // print for test
      val printTemp1 : String = allUsers.collect.map { case (u) => "User " + u } mkString ("\n")
      println(printTemp1)
      val printTemp2 : String = likesCommentUserNil.collect.map {  case (c, us) => "Comment " + c + " : " + us.map(f => "user" + f + ", ")} mkString ("\n")
      println(printTemp2)
      /*
      /** combine old and new friendships  */
      val newFriendships : RDD[(Long, Long)] = FriendshipsRDD.map(f => (f.user_id_1, f.user_id_2))
      val allFriendships : RDD[(Long, Long)] = Friendships union newFriendships
      val allFriendshipsRev : RDD[(Long, Long)] = allFriendships map {case (u1, u2) => (u2, u1)}

      /** filter out unnecessary */
      val useFriendships : RDD[(Long, Long)] = allFriendships join allUsersPair values
      val useFriendshipsRev : RDD[(Long, Long)] = allFriendshipsRev join allUsersPair values
      val useFriendshipsAll : RDD[(Long, Long)] = useFriendships union (useFriendshipsRev map {case (u1, u2) => (u2, u1)})

      /** make friendships into map */
/*
      val bothWayAllFriendships : RDD[(Long, Long)] = useFriendshipsAll union (useFriendshipsAll map { case (f1, f2) =>  (f2, f1) })
      val refinedFrienships : RDD[(Long, Iterable[Long])] = bothWayAllFriendships groupByKey
      val allRefinedFrienships : RDD[(Long, Iterable[Long])] = (allUsersPair leftOuterJoin refinedFrienships values) mapValues  {case None => Iterable()}
      // print for test
      val printTemp3 : String = allRefinedFrienships.collect.map { case (u, friends) => "User " + u + " : " + friends.map(f => f + ", ")} mkString ("\n")
      println(printTemp3)
*/

      /** making into graph */
      val friendships : Array[(Long, Long)] = useFriendshipsAll.collect()
      val commentFriendships : RDD[(Long, Iterable[(Long, Long)])] = likesCommentUserNil mapValues
        ( us => friendships filter { case (u1, u2) => (us exists (_ == u1)) || (us exists (_ == u2))})
      val commentLikeFrienship : RDD[(Long, (Iterable[Iterable[Long]], Iterable[(Long, Long)]))] =
        (likesCommentUserNil mapValues (u => Iterable(u))) join commentFriendships

      def graphRec (vertex : Set[Set[Long]], edges : Iterable[(Long, Long)]): Set[Set[Long]] = {
        if (edges isEmpty) vertex
        else {
          val e = edges.head
          val graph1 = vertex find (_.contains(e._1)) get
          val graph2 = vertex find (_.contains(e._2)) get

          if (graph1 == graph2)
            graphRec(vertex, edges.tail)
          else {
            val rmGraph = vertex - graph1 - graph2
            val mkGraph = rmGraph + (graph1 union graph2)
            graphRec(mkGraph, edges.tail)
          }
        }
      }
      val refinedType : RDD[(Long, (Set[Set[Long]], List[(Long, Long)]))] =
        commentLikeFrienship mapValues {case(v, e) => (v.map(_.toSet).toSet, e.toList) }
      val graph : Array[(Long, Set[Set[Long]])] = (refinedType collect) map {case(c, (v, e)) => (c,graphRec(v, e))}
      val graphSize : Array[(Long, Int)] = graph map { case (c, vs) => (c, vs.map(_.size).max)}
      // print for test
      val printTemp4 : String = graphSize map { case (c, s) => "Comment : " + c + "\tSize : " + s} mkString ("\n")
      println(printTemp4)

*/
      /** processes regards to date */
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24) // 하루 지남
      currentDate = new Timestamp(date.getTime())
      daysTimestamp map (_.decrease()) // decrease the scores of the old dates
      val filteredTS = daysTimestamp filter (ts => (ts.score > 0))

      main_recur( i+1, k, d, Friendships, new dataTypes.Timestamp(date) :: filteredTS)
    }

    println(timing)
    sc.stop()
  }
}