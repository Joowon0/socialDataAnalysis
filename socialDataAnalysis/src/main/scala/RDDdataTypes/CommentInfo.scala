package RDDdataTypes

import java.sql.Timestamp

case class CommentInfo(timestamp : Timestamp, comment_id : Long, user_id : Long,
                       comment : String, user : String,
                       comment_replied : Long, post_commented : Long) {
  var score = 10
  def getScore() : Int = score

  def decrease(): Unit = {
    score = score - 1
  }

  override def toString: String = {
    val front =
      score + "|" +
        timestamp + "|" +
        comment_id + "|" +
        user_id + "|"
    val middle =
      if (comment != 0)
        comment + "|" + user + "|"
      else
        "|"+ user + "|"
    val back =
      if (comment_replied != 0)
        comment_replied + "|"
      else
        "|" + post_commented

    front + middle + back
  }
}