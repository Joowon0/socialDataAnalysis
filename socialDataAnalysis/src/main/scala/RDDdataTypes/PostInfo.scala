package RDDdataTypes

import java.sql.Timestamp

case class PostInfo(timestamp : Timestamp, post_id : Long,
                    user_id : Long, post : String, user : String) {
  var score = 10
  def getScore() : Int = score

  def decrease(): Unit = {
    score = score -1
  }

  override def toString: String = {
    val front =
      score + "|" +
        timestamp + "|" +
        post_id + "|" +
        user_id + "|"

    val back =
      if (post != 0)
        post + "|" + user
      else
        "|" + user

    front + back
  }
}