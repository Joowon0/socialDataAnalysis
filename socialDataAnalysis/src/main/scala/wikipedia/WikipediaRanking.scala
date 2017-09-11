package wikipedia

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import realTime._
import libFromCoursera._


case class CommentInfo(timestamp : Timestamp, comment_id : Long, user_id : Long,
                       comment : String, user : String,
                       comment_replied : Long, post_commented : Long) {}

case class FriendshipInfo(timestamp : Timestamp, user_id_1 : Long,
                          user_id_2 : Long) {}

case class PostInfo(timestamp : Timestamp, post_id : Long,
                    user_id : Long, post : String, user : String) {}

case class LikeInfo(timestamp : Timestamp, user_id : Long, comment_id : Long) {}


object WikipediaRanking {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis")
  val sc: SparkContext = new SparkContext(conf)

  val commentsRDD: RDD[CommentInfo] = sc.textFile("/home/data/comments.dat").map(CommentsData.parse)
  val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/data/friendships.dat").map(FriendshipsData.parse)
  val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/data/likes.dat").map(LikesData.parse)
  val PostsRDD: RDD[PostInfo] = sc.textFile("/home/data/posts.dat").map(PostsData.parse)

  //commentsRDD.sortBy(_.timestamp.getNanos) // 코멘트 타임스탬프로 정렬
  //PostsRDD.sortBy(_.timestamp.getNanos) // 포스트 타임스탬프로 정렬
  var commentList: List[CommentInfo] = commentsRDD.collect().toList // 코멘트 리스트
  var postList: List[PostInfo] = PostsRDD.collect().toList // 포스트 리스트



  def processPost (currentDate : Timestamp, dayStamp : realTime.Timestamp) : Unit = {
    var exec = true
    while (exec) {
      exec = false
      if (postList.length != 0 && postList.head.timestamp.before(currentDate)) {
        exec = true

        Query1.posts + new Post(postList.head.post_id, dayStamp)
        postList = postList.tail
      }
    }
  }

  def processComment (currentDate : Timestamp, dayStamp : realTime.Timestamp) : Unit = {
    var exec = true
    while (exec) {
      exec = false
      if (commentList.length != 0 && commentList.head.timestamp.before(currentDate)) {
        exec = true

        val c = commentList.head
        commentList = commentList.tail

        val postOrigin: Post =
          if (c.comment_replied == 0)
            (Query1.posts find (p => p.PostID == c.post_commented)).get
          else
            Query1.connectedPost(c.comment_replied)

        postOrigin.addComment(new Comment(c.comment_id, dayStamp, c.timestamp))
        Query1.postsUpdate += postOrigin
      }
    }
  }

  def main(args: Array[String]) {
    val df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse("2010-01-01T03:00:00.000+0000") // 12시 정오임

    //2010-03-01T12:00:00.000
    var currentDate : Timestamp = new Timestamp(date.getTime()) // java.util.Date
    Query1.daysTimestamp + new realTime.Timestamp(date)         // realTime.timestamp defined here

    var i = 0
    //while (true) {
    while (i < 10) {
      i = i + 1
      //println("현재 날짜 : " + currentDate.toString)

      /** put data in queue */
      parallel (processPost(currentDate, Query1.daysTimestamp.head),
                processComment(currentDate, Query1.daysTimestamp.head))

      /** calculate */
      Threads.postRealTime(100)

      //print("\n")

      /** processes regards to date */
      val date : Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24)
      // 하루 지남
      currentDate = new Timestamp(date.getTime())
      // decrease the scores of the old dates
      Query1.daysTimestamp map (_.decrease())
      // add a current date
      Query1.daysTimestamp += new realTime.Timestamp(date)
    }

    Query1.TOP3.getTopPosts() map {p => println(p.PostID)}
    println("\n")

    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}