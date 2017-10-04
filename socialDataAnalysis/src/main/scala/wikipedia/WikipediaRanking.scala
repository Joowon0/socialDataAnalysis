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

  //var CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments.dat").map(CommentsData.parse)
  //var FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships.dat").map(FriendshipsData.parse)
  //var LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes.dat").map(LikesData.parse)
  //var PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/posts.dat").map(PostsData.parse)



  def main(args: Array[String]) {
    val df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse("2010-01-01T03:00:00.000+0000") // 12시 정오임

    //2010-02-01T12:00:00.000
    var currentDate : Timestamp = new Timestamp(date.getTime()) // java.util.Date

    var i = 0
    /** Don't know why start at Jan 01 */
    while(i < 29) {
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
    while (i < 10) {
      i = i + 1
      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments"+i+".dat").map(CommentsData.parse)
      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships"+i+".dat").map(FriendshipsData.parse)
      val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes"+i+".dat").map(LikesData.parse)
      val PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/posts"+i+".dat").map(PostsData.parse)

      // print test
      println("현재 날짜    : " + currentDate.toString)
      val printTemp :String = (Query1.daysTimestamp map (x => x.toString)).mkString(" ")
      println("timestamps : " + printTemp)


      /** trying Query1 in distributed way but failed*/
/*
      val postKeyID = PostsRDD.map (p => (p.post_id, p))

      val withPostID    = CommentsRDD.filter(c => c.post_commented  != 0).map(c => (c.post_commented, c)).groupByKey()
      val withoutPostID = CommentsRDD.filter(c => c.comment_replied != 0).map(c => (c.comment_replied, c)).groupByKey()

      val postsComments : RDD[(PostInfo, Option[Iterable[CommentInfo]])] =
        postKeyID.leftOuterJoin(withPostID).values
*/

      /** connect comments to according posts */
      CommentsRDD.collect().map(c => (c,
        if (c.comment_replied == 0) // comment on post
          Query1.insertConnection(c.comment_id, c.post_commented)
        else { // comment on comment
          val postedID : Long = Query1.connectedPost(c.comment_replied)
          Query1.insertConnection(c.comment_id, postedID)
        }
      ))
      // commentID to PostID
      val commentPost : RDD[(Long, Long)] = sc.parallelize(Query1.connectedPost.toSeq)
      val printTemp1 : String = commentPost.collect.map { case (c,p) => c + " : " + p} mkString ("\n")
      println(printTemp1)

      /** refine posts RDD */
      val posts : RDD[(Long, Post)] = PostsRDD.map{ p => (p.post_id, new Post(p.post_id, Query1.daysTimestamp.head))}

      /** refine comments RDD */
      val allComments : RDD[Comment] = CommentsRDD.map(c => new Comment(c.comment_id, Query1.daysTimestamp.head, c.timestamp))
      val comments : RDD[(Long, Comment)] = allComments.map{ c => (c.commentID, c)}

      /** extract posts and according comments */
      val postCommentID : RDD[(Long, Iterable[Comment])] = commentPost.join(comments).values.groupByKey()
      val postComment : RDD[(Post, Option[Iterable[Comment]])] = posts.leftOuterJoin(postCommentID).values
      postComment.map{
        case(p, None) => "Post : " + p + "\nComment : X"
        case(p, c) => "Post : " + p + "\nComment : " + c}
        .collect().foreach(println)

      /** calculate scores */
      val scores : RDD[(Int, (Post, Option[Iterable[Comment]]))] = {
        def addScore(score: Int, comment: Comment): Int =
          comment.getScore() + score

        postComment.map {
          case (post, None) => (post.getScore(), (post, None))
          case (post, comments) =>
          val scores = post.getScore() + comments.get.aggregate(0)(addScore, _ + _)

          (scores, (post, comments))
        }
      }

      /** get max */
      val sorted = scores.sortByKey()
      sorted.map{
        case(s, (p, None)) => "Score : " + s + "\nPost : " + p + "\nComment : X"
        case(s, (p, c)) => "Score : " + s + "\nPost : " + p + "\nComment : " + c}
        .collect().foreach(println)
      //val extractedTop3 : Array[(Int, (Post, Option[Iterable[Comment]]))] = sorted.take(3)
      //val top3 : Array[Post] = extractedTop3 map{ case( score, (post, comments)) => post}


      /** calculate */
      //val printTemp3: String = top3 map {p => p.PostID} mkString (" ")
      //println("TOP3 : " + printTemp3)

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

  def main2(args: Array[String]): Unit = {

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
