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
  //val conf: SparkConf = new SparkConf().setMaster("spark://192.168.0.195:7077").setAppName("twitterAnalysis")
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis")
  val sc: SparkContext = new SparkContext(conf)

  val commentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments.dat").map(CommentsData.parse)
  val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships.dat").map(FriendshipsData.parse)
  val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes.dat").map(LikesData.parse)
  val PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/posts.dat").map(PostsData.parse)

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

        //println(postList.head)

        Query1.posts += new Post(postList.head.post_id, dayStamp)
        postList = postList.tail
      }
    }
    println(Query1.posts)
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

        //println((c, postOrigin))

        postOrigin.addComment(new Comment(c.comment_id, dayStamp, c.timestamp))
        Query1.insertConnection(c.comment_id, postOrigin)
        Query1.postsUpdate += postOrigin
      }
    }
    println("total comment : " + Query1.postsUpdate)
  }

  def main(args: Array[String]) {
    val df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse("2010-01-01T03:00:00.000+0000") // 12시 정오임

    //2010-02-01T12:00:00.000
    var currentDate : Timestamp = new Timestamp(date.getTime()) // java.util.Date

    var i = 0
    /** Don't know why start at Jan 01 */
    while(i < 30) {
      i = i + 1
      val date : Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24)
      currentDate = new Timestamp(date.getTime())
    }
    val date2 : Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 36)
    currentDate = new Timestamp(date2.getTime())
    new realTime.Timestamp(date2) +=: Query1.daysTimestamp       // realTime.timestamp defined here


    //lazy val printTemp : String = postList map (x => x.toString) mkString  ("\n")
    //println(printTemp)

    i = 0
    //while (true) {
    while (i < 100) {
      i = i + 1
      println("현재 날짜    : " + currentDate.toString)

      val printTemp :String = (Query1.daysTimestamp map (x => x.toString)).mkString(" ")
      println("timestamps : " + printTemp)

      /** put data in queue */
      processPost(currentDate, Query1.daysTimestamp.head)
      //val printTemp2 :String = (Query1.posts map (x => x.toString)).mkString(" ")
      //println("all posts  : \n" + printTemp2)

      processComment(currentDate, Query1.daysTimestamp.head)
/*
      println(Query1.posts)

      /** calculate */
      Threads.postRealTime(100)
*/
      println()

      /** processes regards to date */
      val date : Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24)
      // 하루 지남
      currentDate = new Timestamp(date.getTime())
      // decrease the scores of the old dates
      Query1.daysTimestamp map (_.decrease())
      // add a current date
      new realTime.Timestamp(date) +=: Query1.daysTimestamp
    }

    //Query1.TOP3.getTopPosts() map {p => println(p.PostID)}
    //println("\n")

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