package wikipedia

import java.io.File
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

/**
  * Created by syndr on 2017-08-09.
  */
object CommentsData {
  private[wikipedia] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("data/comments.dat")
    if (resource == null) sys.error("comments.dat == null")
    new File(resource.toURI).getPath
  }

  private[wikipedia] def parse(line: String): CommentInfo = {
    val dat = line.split("|").toList
    val ts = dat(0).split("+").toList.head.split("T")
    //val date : String = ts(0)
    //val time : String = ts(1)

    val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSX")
    val date: Date = df.parse(dat(0))
    val timestamp: Timestamp = new Timestamp(date.getTime())
    val comment_id: Long = dat(1).toLong
    val user_id: Long = dat(2).toLong
    val comment: String = dat(3)
    val user: String = dat(4)
    val comment_replied: Long = dat(5).toLong
    val post_commented: Long = dat(6).toLong
    CommentInfo(timestamp, comment_id, user_id, comment, user, comment_replied, post_commented)
  }
}
