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

  val conf : SparkConf = new SparkConf().Set
}
