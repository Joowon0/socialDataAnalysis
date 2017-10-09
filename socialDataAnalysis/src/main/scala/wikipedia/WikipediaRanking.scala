package wikipedia

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.{CommentInfo, PostInfo}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WikipediaRanking {
/*
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
*/

  def main(args: Array[String]) {

    Query2.q2(args)

    /*

    val df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse("2010-01-01T03:00:00.000+0000") // 12시 정오임

    //2010-02-01T12:00:00.000
    var currentDate : Timestamp = new Timestamp(date.getTime()) // java.util.Date

    var i = 0
    /** Don't know why start at Jan 01 */
    while (i < 30) {
      i = i + 1
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24)
      currentDate = new Timestamp(date.getTime())
    }
    val date2: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 36)
    currentDate = new Timestamp(date2.getTime())

    /** calling recursive function */
    main_recur(1, sc.emptyRDD, Map()/*, List(new dataTypes.Timestamp(date2))*/)

    /**
      * @param i             - count the number of recursion
      * @param Posts         - Structures to store all Posts
      * @param connectedPost - given a comment ID, able to find corresponding posts
      * //@param daysTimestamp - all of timestamps
      */
    def main_recur(i : Int, Posts : RDD[(PostInfo, Set[CommentInfo])], connectedPost : Map[Long, Long]/*, daysTimestamp : List[dataTypes.Timestamp]*/) {
      if (i > 179) return

      /** RDD read from file */
      val PostsRDD: RDD[PostInfo] = sc.textFile("/home/ana/data/data_day/posts/posts" + i + ".dat").map(PostsData.parse)
      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/data_day/comments/comments" + i + ".dat").map(CommentsData.parse)
      //val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/data_day/friendships/friendships" + i + ".dat").map(FriendshipsData.parse)
      //val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/data_day/likes/likes" + i + ".dat").map(LikesData.parse)

      // print test
//      println("현재 날짜    : " + currentDate.toString)
//      val printTemp: String = (daysTimestamp map (x => x.toString)).mkString(" ")
//      println("timestamps : " + printTemp)

      /** connect comments to according posts (sequential part) */
      val commentTemp = CommentsRDD.collect()
      val commentSize = commentTemp.length

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
      val oldPosts: RDD[(Long, PostInfo)] = Posts.keys.map { p => (p.post_id, p) }
      val newPosts: RDD[(Long, PostInfo)] = PostsRDD.map { p => (p.post_id, p) }
      val allPosts: RDD[(Long, PostInfo)] = oldPosts union newPosts

      /** refine comments RDD */
      //val newRefinedComments: RDD[CommentInfo] = CommentsRDD.map(c => new Comment(c.comment_id, daysTimestamp.head))
      val newComments: RDD[(Long, CommentInfo)] = CommentsRDD.map { c => (c.comment_id, c) }

      /** extract posts and according comments */
      val newPostIDComment: RDD[(Long, Iterable[CommentInfo])] = commentPost.join(newComments).values.groupByKey()
      val newPostComment: RDD[(PostInfo, Option[Iterable[CommentInfo]])] = allPosts.leftOuterJoin(newPostIDComment).values
      val newRefinedPostComment: RDD[(PostInfo, Set[CommentInfo])] = newPostComment.map { case (p, iter) => (p, iter.toSet.flatten) }
      val allPostComment: RDD[(PostInfo, Set[CommentInfo] )] = Posts union newRefinedPostComment
      val groupAllPostComment: RDD[(PostInfo, Set[CommentInfo])] = {
        val group =  allPostComment.groupBy { case (post, value) => post.post_id }.values
        val refine: RDD[(PostInfo, Set[CommentInfo])] =
          group.map {
            case set =>
              val comments = set.flatMap { case (post, eachSet) => eachSet }
              (set.head._1, comments.toSet)
          }
        refine
      }

      /** function that calculate scores */
      val scores =
        (post2Comment: (PostInfo, Set[CommentInfo])) => {
          def addScore =
            (score: Int, comment: CommentInfo) => comment.getScore() + score

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
      val sorted: RDD[(Int, (PostInfo, Set[CommentInfo]))] = groupAllPostComment.map(rdd => (scores(rdd), rdd)).sortByKey(false)
      val extractedTop3 : Array[(Int, (PostInfo, Set[CommentInfo]))] =
        if( sorted.count () > 3) sorted.take(3)
        else sorted.collect()

      // print for test
//      val resultPrint = extractedTop3.map {
//        case (s, (p, c)) => "Score : " + s + "\n" + p + "\n" + (c mkString "\n")
//      } . foreach(println)

      /** write to file */
      val resultFile : String = extractedTop3.map {
        case (s, (p, c)) => "Score : " + s + "\n" + p + "\n" + (c mkString "\n")
      } mkString "\n"

      val pw = new PrintWriter(new File("/home/ana/data/query1Out/" + i + ".dat" ))
      pw.write(resultFile )
      pw.close

      //println()
/*

      /** processes regards to date */
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24) // 하루 지남
      currentDate = new Timestamp(date.getTime())
      daysTimestamp map (_.decrease()) // decrease the scores of the old dates
*/

      val decreasedPostComment: RDD[(PostInfo, Set[CommentInfo])] =
        groupAllPostComment.map {
          case (post, comments) =>
            post.decrease()
            val newComments = comments.map{
              case c =>
                c.decrease()
                c
            }
            (post, newComments)
        }

      /** filter posts and timestamp that is under 0 */
      val filteredPosts: RDD[(PostInfo, Set[CommentInfo])] = decreasedPostComment.filter(p => scores(p) > 0)
//      val filteredTS = daysTimestamp filter (ts => (ts.score > 0))

      val filteredCommentID : RDD[Long] = filteredPosts flatMap { case (p, cs) => cs map (_.comment_id)}
      val filteredConnection : RDD[(Long, Long)] =
        (filteredCommentID map {c => (c, c)}) join sc.parallelize(allConnection.toSeq) values
      val filteredConnectionToMap = filteredConnection.collect().toMap

      /** 야매 */
      //val filteredPosts2: RDD[(PostInfo, Set[CommentInfo])] = sc.parallelize(extractedTop3.toSeq) values

      main_recur( i+1, filteredPosts, filteredConnectionToMap/*, new dataTypes.Timestamp(date) :: filteredTS*/)
    }

    println(timing)
    sc.stop()*/
  }
}
