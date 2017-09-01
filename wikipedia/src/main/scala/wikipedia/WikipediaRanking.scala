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
  /*
  * 여기에 필요한 함수 정의
  * */
}

case class FriendshipInfo(timestamp : Timestamp, user_id_1 : Long,
                          user_id_2 : Long) {
  /*
  * 여기에 필요한 함수 정의
   */
}

case class PostInfo(timestamp : Timestamp, post_id : Long,
                    user_id : Long, post : String, user : String) {
  /*
  * 여기에 필요한 함수 정의
   */
}

case class LikeInfo(timestamp : Timestamp, user_id : Long, comment_id : Long) {
  /*
  * 여기에 필요한 함수 정의
   */
}

object WikipediaRanking {

  /*val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")*/


  /*val conf: SparkConf = ???
  val sc: SparkContext = ???*/
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`


  //val wikiRdd: RDD[WikipediaArticle] = ???
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

  /** Returns the number of articles on which the language `lang` occurs.
    * Hint1: consider using method `aggregate` on RDD[T].
    * Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
    */
  ///def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = ???

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */

  //val threePosts : ThreePosts

  /*def findTop3(commentsRDD : RDD[CommentInfo], postsRDD : RDD[PostInfo]) : List[Post] = {
    //threePosts.getTopPosts()
  }*/
  /*
   * 쓰레드 돌려서 threePosts 변수에 타임스탬프 순으로 insert해줌. 중간에 findTop3하면 현재 시점에서의 top3 포스트가 나옴
   * queue 만들어서 posts를 타임스탬프 순서대로 넣으면 됨
   */

  def findTop3(commentRDD: RDD[CommentInfo], postsRDD: RDD[PostInfo]): List[(String, Int)] = ???


  ///def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = ???

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  ///def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = ???

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  ///def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = ???

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  ///def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = ???

  /*def postIsPast(post : Post): Unit = {
    post.timestamp.
  }*/

  def main(args: Array[String]) {
    //println("HELP")

    //val query1 : List[(String, Int)] = timed("Query 1 : find top 3 posts", findTop3())

    var df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
    var date : Date = df.parse("2010-01-01T03:00:00.000+0000")

    //2010-03-01T12:00:00.000
    var currentDate : Timestamp = new Timestamp(date.getTime())
    currentDate = new Timestamp(new Date(currentDate.getTime() + 1000 * 60 * 60 * 24 * 20).getTime())
    currentDate = new Timestamp(new Date(currentDate.getTime() + 1000 * 60 * 60 * 24 * 11).getTime())
    //commentsRDD.to
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
          println("post timestamp : " + postList.head.timestamp)
          postList = postList.tail
        }

        /*if (Queue.newComment.head.timestamp.isPast(currentDate)) {
          exec = true
          //Queue.newComment.head 로 연산하고 tail을 Queue.newComment = Queue.newComment.tail 해주면됨
        }*/
        //print(".")
      }
      print("\n")
      currentDate = new Timestamp(new Date(currentDate.getTime() + 1000 * 60 * 60 * 24).getTime()) // 하루 지남
    }
    //Queue.newPosts()
    //--------------------------------------------------------------------------------------

    /*
    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()*/

    /* Query 1 */
    //val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    //def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    //val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    //val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
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
