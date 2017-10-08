package wikipedia

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.{CommentInfo, FriendshipInfo, LikeInfo, PostInfo}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import dataTypes._

object WikipediaRanking {
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
    main_recur(1, sc.emptyRDD, Map(), List(new dataTypes.Timestamp(date2)))

    /**
      * @param i              - count the number of recursion
      * @param Posts          - Structures to store all Posts
      * @param connectedPost - given a comment ID, able to find corresponding posts
      * @param daysTimestamp - all of timestamps
      */
    def main_recur(i : Int, Posts : RDD[(Post, Set[Comment])], connectedPost : Map[Long, Long], daysTimestamp : List[dataTypes.Timestamp]) {
      if (i > 20) return

      /** RDD read from file */
      val CommentsRDD: RDD[CommentInfo] = sc.textFile("src/main/scala/data_day/comments/comments" + i + ".dat").map(CommentsData.parse)
      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("src/main/scala/data_day/friendships/friendships" + i + ".dat").map(FriendshipsData.parse)
      val LikesRDD: RDD[LikeInfo] = sc.textFile("src/main/scala/data_day/likes/likes" + i + ".dat").map(LikesData.parse)
      val PostsRDD: RDD[PostInfo] = sc.textFile("src/main/scala/data_day/posts/posts" + i + ".dat").map(PostsData.parse)
//      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/comments" + i + ".dat").map(CommentsData.parse)
//      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/friendships" + i + ".dat").map(FriendshipsData.parse)
//      val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/likes" + i + ".dat").map(LikesData.parse)
//      val PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/posts" + i + ".dat").map(PostsData.parse)

      // print test
      println("현재 날짜    : " + currentDate.toString)
      val printTemp: String = (daysTimestamp map (x => x.toString)).mkString(" ")
      println("timestamps : " + printTemp)

      /** connect comments to according posts (sequential part) */
      val commentTemp = CommentsRDD.collect()
      val commentSize = commentTemp.length
//      println("comment num : " + commentSize)
//      commentTemp.foreach(println)


      def connect(connection : Map[Long, Long], index : Int): Map[Long, Long] = {
        if (index >= commentSize) connection
        else {
          val c = commentTemp(index)
          if (c.comment_replied == 0)
            connect(connection + (c.comment_id -> c.post_commented), index + 1)
          else {
            val postedID: Long = connection(c.comment_replied)
            connect(connection + (c.comment_id -> postedID), index + 1)
          }
        }
      }
      val allConnection = connect(connectedPost, 0)
      val commentPost: RDD[(Long, Long)] = sc.parallelize(allConnection.toSeq)

      /** refine posts RDD */
      val oldPosts: RDD[(Long, Post)] = Posts.keys.map { p => (p.PostID, p) }
      val newPosts: RDD[(Long, Post)] = PostsRDD.map { p => (p.post_id, new Post(p.post_id, daysTimestamp.head)) }
      val allPosts: RDD[(Long, Post)] = oldPosts union newPosts

      /** refine comments RDD */
      val newRefinedComments: RDD[Comment] = CommentsRDD.map(c => new Comment(c.comment_id, daysTimestamp.head, c.timestamp))
      val newComments: RDD[(Long, Comment)] = newRefinedComments.map { c => (c.commentID, c) }

      /** extract posts and according comments */
      val newPostIDComment: RDD[(Long, Iterable[Comment])] = commentPost.join(newComments).values.groupByKey()
      val newPostComment: RDD[(Post, Option[Iterable[Comment]])] = allPosts.leftOuterJoin(newPostIDComment).values
      val newRefinedPostComment: RDD[(Post, Set[Comment])] = newPostComment.map { case (p, iter) => (p, iter.toSet.flatten) }
      val allPostComment: RDD[(Post, Set[Comment] )] = Posts union newRefinedPostComment
      val groupAllPostComment: RDD[(Post, Set[Comment])] = {
        val group =  allPostComment.groupBy { case (post, value) => post.PostID }.values
        val refine: RDD[(Post, Set[Comment])] =
          group.map {
            case set =>
              val comments = set.flatMap { case (post, eachSet) => eachSet }
              (set.head._1, comments.toSet)
          }
        refine
      }

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
      //val scoreUpdate = allPostComment.map{ case(post, comments) => post.getScore}
      /** get max */
      val sorted: RDD[(Int, (Post, Set[Comment]))] = groupAllPostComment.map(rdd => (scores(rdd), rdd)).sortByKey()
      sorted.map {
        case (s, (p, c)) => "Score : " + s + "\nPost : " + p + "Comment : " + c + "\n"
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

      val decreasedPostComment: RDD[(Post, Set[Comment])] =
        groupAllPostComment.map {
          case (post, comments) =>
            post.decrease()
            comments.map{ case c => c.decrease()}
            (post, comments)
        }

      /** filter posts that is under 0 */
      val filteredPosts = decreasedPostComment.filter(p => scores(p) > 0)

      main_recur( i+1, filteredPosts, allConnection, new dataTypes.Timestamp(date) :: daysTimestamp)
    }

    println(timing)
    sc.stop()
  }
}
