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
    main_recur(1, sc.emptyRDD, List(new dataTypes.Timestamp(date2)), Map())


    /**
      * @param i - count the number of recursion
      * @param k - number of post we want to get
      * @param d - within k seconds
      * @param Comments    - Post ID  -> Set of Comments
      * @param Likes       - Comments -> Likes
      * @param Friendships - User ID  -> Set of user id
      */
    def main_recur(i : Int, k: Int, d: Int, Comments : RDD[(Long, Set[Comment])], Likes : RDD[(Long, Long)], Friendships : RDD[(Long, Iterable[Long])]] ) {
      if (i > 11) return

      /** RDD read from file */
      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments" + i + ".dat").map(CommentsData.parse)
      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships" + i + ".dat").map(FriendshipsData.parse)
      val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes" + i + ".dat").map(LikesData.parse)
      val PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/posts" + i + ".dat").map(PostsData.parse)

      // print test
      println("현재 날짜    : " + currentDate.toString)
      val printTemp: String = (daysTimestamp map (x => x.toString)).mkString(" ")
      println("timestamps : " + printTemp)


      /** processes regards to date */
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24) // 하루 지남
      currentDate = new Timestamp(date.getTime())
      daysTimestamp map (_.decrease()) // decrease the scores of the old dates

      val decreasedPostComment: RDD[(Post, Set[Comment])] =
        groupAllPostComment.map {
          case (post, comments) =>
            post.decrease()
            comments.map{ case c => c.decrease()}
            (post, comments)
        }

      main_recur( i+1, filteredPosts, new dataTypes.Timestamp(date) :: daysTimestamp, allConnection)
    }

    println(timing)
    sc.stop()
  }
}