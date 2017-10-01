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

  val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments.dat").map(CommentsData.parse)
  val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships.dat").map(FriendshipsData.parse)
  val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes.dat").map(LikesData.parse)
  val PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/posts.dat").map(PostsData.parse)



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
      //processPost(currentDate, Query1.daysTimestamp.head)
      /** Query1.posts = PostsRDD*/
      //PostsRDD.foreach(p => Query1.posts += new Post(p.post_id, Query1.daysTimestamp.head))
      //val printTemp2 :String = (Query1.posts map (x => x.toString)).mkString(" ")
      //println("all posts  : \n" + printTemp2)


      val postKeyID = PostsRDD.map (p => (p.post_id, p))

      val withPostID    = CommentsRDD.filter(c => c.post_commented  != 0).map(c => (c.post_commented, c)).groupByKey()
      val withoutPostID = CommentsRDD.filter(c => c.comment_replied != 0).map(c => (c.comment_replied, c)).groupByKey()

      val postsComments : RDD[(PostInfo, Option[Iterable[CommentInfo]])] =
        postKeyID.leftOuterJoin(withPostID).values





      val comment2Post = CommentsRDD.map(c => (c,
        if (c.comment_replied == 0)
          (Query1.posts find (p => p.PostID == c.post_commented)).get
        else
          Query1.connectedPost(c.comment_replied)))

      comment2Post.foreach{ case(c, p) => p.addComment(new Comment(c.comment_id, Query1.daysTimestamp.head, c.timestamp)) }
      comment2Post.foreach{ case(c, p) => Query1.insertConnection(c.comment_id, p) }
      comment2Post.foreach{ case(c, p) => Query1.postsUpdate += p}


      /** calculate */
      Threads.postRealTime(0)
      val printTemp3: String = Query1.TOP3.getTopPosts() map {p => p.PostID} mkString (" ")
      println("TOP3 : " + printTemp3)

      println()

      /** processes regards to date */
      val date : Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24)
      // 하루 지남
      currentDate = new Timestamp(date.getTime())
      // decrease the scores of the old dates
      Query1.daysTimestamp map (_.decrease())
      // add a current datePostsRDD.groupBy(p => p.timestamp)
      new realTime.Timestamp(date) +=: Query1.daysTimestamp
    }

    //Query1.TOP3.getTopPosts() map {p => println(p.PostID)}
    //println()

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