package wikipedia

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.{CommentInfo, FriendshipInfo, LikeInfo, PostInfo}
import RDDdataflow.{CommentsData, FriendshipsData, LikesData, PostsData}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import dataTypes._

import scala.collection.mutable


object WikipediaRanking {
  //val conf: SparkConf = new SparkConf().setMaster("spark://192.168.0.195:7077").setAppName("twitterAnalysis")
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis")
  val sc: SparkContext = new SparkContext(conf)

  /** Structures to store all Posts */
  var Posts : RDD[(Post, Set[Comment])] = sc.emptyRDD
  /** all of timestamps */
  val daysTimestamp : mutable.MutableList[dataTypes.Timestamp] = mutable.MutableList()
  /** given a comment ID, able to find corresponding posts */
  var connectedPost : Map[Long, Long] = Map() // withDefaultValue (-1)
  def insertConnection(commentID: Long, postID: Long) : Unit =
    connectedPost = connectedPost + (commentID -> postID)

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
    new dataTypes.Timestamp(date2) +=: daysTimestamp // realTime.timestamp defined here

    i = 0
    //while (true) {
    while (i < 10) {
      i = i + 1
      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments" + i + ".dat").map(CommentsData.parse)
      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships" + i + ".dat").map(FriendshipsData.parse)
      val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes" + i + ".dat").map(LikesData.parse)
      val PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/posts" + i + ".dat").map(PostsData.parse)

      // print test
      println("현재 날짜    : " + currentDate.toString)
      val printTemp: String = (daysTimestamp map (x => x.toString)).mkString(" ")
      println("timestamps : " + printTemp)

      /** connect comments to according posts */
      CommentsRDD.collect().map(c => (c,
        if (c.comment_replied == 0) // comment on post
          insertConnection(c.comment_id, c.post_commented)
        else { // comment on comment
          val postedID: Long = connectedPost(c.comment_replied)
          insertConnection(c.comment_id, postedID)
        }
      ))
      val commentPost: RDD[(Long, Long)] = sc.parallelize(connectedPost.toSeq)
      //val printTemp1 : String = commentPost.collect.map { case (c,p) => c + " : " + p} mkString ("\n")
      //println(printTemp1)

      /** refine posts RDD */
      val oldPosts: RDD[(Long, Post)] = Posts.keys.map { p => (p.PostID, p) }
      val newPosts: RDD[(Long, Post)] = PostsRDD.map { p => (p.post_id, new Post(p.post_id, daysTimestamp.head)) }
      val posts: RDD[(Long, Post)] = oldPosts union newPosts

      /** refine comments RDD */
      val newRefinedComments: RDD[Comment] = CommentsRDD.map(c => new Comment(c.comment_id, daysTimestamp.head, c.timestamp))
      val newComments: RDD[(Long, Comment)] = newRefinedComments.map { c => (c.commentID, c) }

      /** extract posts and according comments */
      val postCommentID: RDD[(Long, Iterable[Comment])] = commentPost.join(newComments).values.groupByKey()
      val newPostComment: RDD[(Post, Option[Iterable[Comment]])] = posts.leftOuterJoin(postCommentID).values
      val newRefinedPostComment: RDD[(Post, Set[Comment])] = newPostComment.map { case (p, iter) => (p, iter.toSet.flatten) }
      val allPostComment: RDD[(Post, Set[Comment])] = (Posts union newRefinedPostComment).groupByKey().map {
        case (p, set) => (p, set.flatten.toSet)
      }
      allPostComment.map {
        case (p, c) => "Post : " + p + "\nComment : " + c
      }.collect().foreach(println)

      /** function that calculate scores */
      val scores =
        (post2Comment: (Post, Set[Comment])) => {
          def addScore =
            (score: Int, comment: Comment) => comment.getScore() + score

          post2Comment match {
            case (post, comments) => {
              if (comments.isEmpty)
                post.getScore()
              else
                post.getScore() + comments.aggregate(0)(addScore, _ + _)
            }
          }
        }

      /** get max */
      val sorted: RDD[(Int, (Post, Set[Comment]))] = allPostComment.map(rdd => (scores(rdd), rdd)).sortByKey()
      sorted.map {
        case (s, (p, c)) => "Score : " + s + "\nPost : " + p + "\nComment : " + c
      }.collect().foreach(println)
      //val extractedTop3 : Array[(Int, (Post, Option[Iterable[Comment]]))] = sorted.take(3)
      //val top3 : Array[Post] = extractedTop3 map{ case( score, (post, comments)) => post}


      /** calculate */
      //val printTemp3: String = top3 map {p => p.PostID} mkString (" ")
      //println("TOP3 : " + printTemp3)

      println()

      /** processes regards to date */
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24) // 하루 지남
      currentDate = new Timestamp(date.getTime())
      daysTimestamp map (_.decrease()) // decrease the scores of the old dates
      new dataTypes.Timestamp(date) +=: daysTimestamp // add a current datePostsRDD.groupBy(p => p.timestamp)

      /** filter posts that is under 0 */
      val filteredPosts = Posts.filter(p => scores(p) > 0)
      Posts = filteredPosts
    }

    println(timing)
    sc.stop()
  }
}
