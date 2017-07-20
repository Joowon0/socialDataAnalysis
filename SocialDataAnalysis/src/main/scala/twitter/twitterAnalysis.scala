package twitter

import java.sql.Timestamp

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

object twitterAnalysis {
  def main(args: Array[String]) {
    println("Empty Main")
  }
}
