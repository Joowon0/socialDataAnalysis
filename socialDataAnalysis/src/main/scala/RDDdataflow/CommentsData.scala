package RDDdataflow

import java.io.File
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.CommentInfo

/**
  * Created by syndr on 2017-08-09.
  */


object CommentsData {
  private[wikipedia] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("./data/comments.dat")
    if (resource == null) sys.error("comments.dat == null")
    new File(resource.toURI).getPath
  }

  private[wikipedia] def parse(line: String): CommentInfo = {
    var dat : Array[String]= line.split("\\|")//.toList
    val ts = dat(0).split("\\+").toList.head.split("T")

    val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSX")
    val date: Date = df.parse(dat(0))

    val timestamp: Timestamp = new Timestamp(date.getTime())
    val comment_id: Long = dat(1).toLong
    val user_id: Long = dat(2).toLong
    val comment: String = dat(3)
    val user: String = dat(4)

    var comment_replied : Long = 0
    if(dat(5) != "") {
      comment_replied = dat(5).toLong
    }

    var post_commented: Long = 0

    if(dat.length == 7)
      post_commented = dat(6).toLong
    /*
    println("-------------------------")
    println("timestamp : " + timestamp)
    println("comment_id : " + comment_id)
    println("user_id : " + user_id)
    println("comment : " + comment)
    println("user : " + user)
    println("comment_replied : " + comment_replied)
    println("post_commented : " + post_commented)
*/
    CommentInfo(timestamp, comment_id, user_id, comment, user, comment_replied, post_commented)
  }
}
