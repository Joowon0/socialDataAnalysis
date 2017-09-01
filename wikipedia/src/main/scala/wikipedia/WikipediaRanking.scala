package wikipedia

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import realTime._

/*case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}*/

case class CommentInfo(timestamp : Timestamp, comment_id : Long, user_id : Long,
                       comment : String, user : String,
                       comment_replied : Long, post_commented : Long) {
}

case class FriendshipInfo(timestamp : Timestamp, user_id_1 : Long,
                          user_id_2 : Long) {
}

case class PostInfo(timestamp : Timestamp, post_id : Long,
                    user_id : Long, post : String, user : String) {
}

case class LikeInfo(timestamp : Timestamp, user_id : Long, comment_id : Long) {
}

object WikipediaRanking {

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis")
  val sc: SparkContext = new SparkContext(conf)

  val commentsRDD: RDD[CommentInfo] = sc.textFile(CommentsData.filePath).map(CommentsData.parse)
  val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile(FriendshipsData.filePath).map(FriendshipsData.parse)
  val LikesRDD: RDD[LikeInfo] = sc.textFile(LikesData.filePath).map(LikesData.parse)
  val PostsRDD: RDD[PostInfo] = sc.textFile(PostsData.filePath).map(PostsData.parse)

  //commentsRDD.sortBy(_.timestamp.getNanos) // 코멘트 타임스탬프로 정렬
  //PostsRDD.sortBy(_.timestamp.getNanos) // 포스트 타임스탬프로 정렬
  var commentList: List[CommentInfo] = commentsRDD.collect().toList // 코멘트 리스트
  var postList: List[PostInfo] = PostsRDD.collect().toList // 포스트 리스트

  PostsRDD map (p => Query1.posts + new Post(p.post_id, ???))
  commentsRDD map { c =>
    val postOrigin: Post =
      if (c.comment_replied == 0)
        (Query1.posts find (p => p.PostID == c.post_commented)).get
      else
        Query1.connectedPost(c.comment_replied)

    postOrigin.addComment(new Comment(c.comment_id, ???, ???))
    Queue.newPosts += postOrigin
  }

  /*def findTop3(commentsRDD : RDD[CommentInfo], postsRDD : RDD[PostInfo]) : List[Post] = {
    //threePosts.getTopPosts()
  }*/
  /*
   * 쓰레드 돌려서 threePosts 변수에 타임스탬프 순으로 insert해줌. 중간에 findTop3하면 현재 시점에서의 top3 포스트가 나옴
   * queue 만들어서 posts를 타임스탬프 순서대로 넣으면 됨
   */

  def findTop3(commentRDD: RDD[CommentInfo], postsRDD: RDD[PostInfo]): List[(String, Int)] = ???

  def main(args: Array[String]) {
    val df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse("2010-01-01T03:00:00.000+0000") // 12시 정오임

    var currentDate : Timestamp = new Timestamp(date.getTime())
    //currentDate = new Timestamp(new Date(currentDate.getTime() + 1000 * 60 * 60 * 24 * 20).getTime())
    //currentDate = new Timestamp(new Date(currentDate.getTime() + 1000 * 60 * 60 * 24 * 11).getTime())

    println("현재 날짜 : " + currentDate.toString)
    while (true) {
      println("현재 날짜 : " + currentDate.toString)
      var exec = true
      while (exec) {
        exec = false
        if (postList.length != 0 && postList.head.timestamp.before(currentDate)) {
          exec = true
          //Queue.newPosts.head 로 연산하고 tail을 Queue.newPosts = Queue.newPosts.tail 해주면됨
          //postList.head 샬라샬라
          println("post timestamp : " + postList.head.timestamp.toString())
          postList = postList.tail
        }
      }
      print("\n")
      currentDate = new Timestamp(new Date(currentDate.getTime() + 1000 * 60 * 60 * 24).getTime()) // 하루 지남
    }

    //println(timing)
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