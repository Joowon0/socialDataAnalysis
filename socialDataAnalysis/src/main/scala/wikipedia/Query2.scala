package wikipedia

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.{CommentInfo, FriendshipInfo, LikeInfo, PostInfo}
import dataTypes._
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
      * @param Friendships   - User ID  -> Set of user id
      * @param daysTimestamp - all of timestamps
      */
    def main_recur(i : Int, k: Int, d: Int, Friendships : RDD[(Long, Long)], daysTimestamp : List[dataTypes.Timestamp] ) {
      if (i > 12) return

      /** RDD read from file */
      val CommentsRDD: RDD[CommentInfo] = sc.textFile("src/main/scala/tempData/comments" + i + ".dat").map(CommentsData.parse)
      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("src/main/scala/tempData/friendships" + i + ".dat").map(FriendshipsData.parse)
      val LikesRDD: RDD[LikeInfo] = sc.textFile("src/main/scala/tempData/likes" + i + ".dat").map(LikesData.parse)
      val PostsRDD: RDD[PostInfo] = sc.textFile("src/main/scala/tempData/posts" + i + ".dat").map(PostsData.parse)
//      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments" + i + ".dat").map(CommentsData.parse)
//      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships" + i + ".dat").map(FriendshipsData.parse)
//      val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes" + i + ".dat").map(LikesData.parse)
//      val PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/posts" + i + ".dat").map(PostsData.parse)

      // print test
      println("현재 날짜    : " + currentDate.toString)
      val printTemp: String = (daysTimestamp map (x => x.toString)).mkString(" ")
      println("timestamps : " + printTemp)

      /** Comments */
      val newComments : RDD[(Long, Long)] = CommentsRDD.map(c=> (c.comment_id, c.comment_id))

      /** Likes */
      val likesCommentUser : RDD[(Long, Long)] = LikesRDD.map(c => (c.comment_id, c.user_id))
      val likes : RDD[(Long, Iterable[Long])] = likesCommentUser groupByKey
      val allLikes : RDD[(Long, Iterable[Long])] = ((newComments leftOuterJoin likes) values) mapValues {case None => Iterable()}

      val allUsers : RDD[Long] = likesCommentUser map (_._2) distinct
      val allUsersPair : RDD[(Long, Long)] = allUsers map (u => (u,u))
      // print for test
      val printTemp1 : String = allUsers.collect.map { case (u) => "User " + u } mkString ("\n")
      println(printTemp1)
      val printTemp2 : String = allLikes.collect.map {  case (c, us) => "Comment " + c + " : " + us.map(f => "user" + f + ", ")} mkString ("\n")
      println(printTemp2)

      /** make friendships into map */
      val newFriendships : RDD[(Long, Long)] = FriendshipsRDD.map(f => (f.user_id_1, f.user_id_2))
      val allFriendships : RDD[(Long, Long)] = Friendships union newFriendships
      val allFriendshipsRev : RDD[(Long, Long)] = allFriendships map {case (u1, u2) => (u2, u1)}

      val useFriendships : RDD[(Long, Long)] = allFriendships join allUsersPair values
      val useFriendshipsRev : RDD[(Long, Long)] = allFriendshipsRev join allUsersPair values
      val useFriendshipsAll : RDD[(Long, Long)] = useFriendships union (useFriendshipsRev map {case (u1, u2) => (u2, u1)})

      val bothWayAllFriendships : RDD[(Long, Long)] = useFriendshipsAll union (useFriendshipsAll map { case (f1, f2) =>  (f2, f1) })
      val refinedFrienships : RDD[(Long, Iterable[Long])] = bothWayAllFriendships groupByKey
      // print for test
      val printTemp3 : String = refinedFrienships.collect.map { case (u, friends) => "User " + u + " : " + friends.map(f => f + ", ")} mkString ("\n")
      println(printTemp3)



      /** processes regards to date */
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24) // 하루 지남
      currentDate = new Timestamp(date.getTime())
      daysTimestamp map (_.decrease()) // decrease the scores of the old dates

      main_recur( i+1, k, d, allFriendships, new dataTypes.Timestamp(date) :: daysTimestamp)
    }

    println(timing)
    sc.stop()
  }
}