package twitter

import java.sql.Timestamp

import org.apache.spark.SparkConf

/**
  * Created by syndr on 2017-07-19.
  */

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

//case class dd() {}

object twitterAnalysis {
  /*def main(args: Array[String]) {
    println("Empty Main")
  }*/

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis");
  val sc: SparkContext = new SparkContext(conf);
  val commentsRDD: RDD[CommentInfo] = sc.textFile(CommentsData.filePath).map(CommentsData.parse)
  val FriendshipsRDD : RDD[FriendshipInfo] = sc.textFile(FriendshipsData.filePath).map(FriendshipsData.parse)
  val LikesRDD : RDD[LikeInfo] = sc.textFile(LikesData.filePath).map(LikesData.parse)
  val PostsRDD : RDD[PostInfo] = sc.textFile(PostsData.filePath).map(PostsData.parse)

  def topThreeComments(): List[(Long, String)] = {
    /*
    * 입력은 인자로 필요한거 받아서 쓰고
    * 출력은 이 안에서 계산해서 id,내용 리스트로 리턴하면 됨
     */
  }
}