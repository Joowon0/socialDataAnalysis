package twitter

import java.io.File
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

/**
  * Created by syndr on 2017-08-09.
  */
object LikesData {
  private[twitter] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("twitter/likes.dat")
    if (resource == null) sys.error("likes.dat == null")
    new File(resource.toURI).getPath
  }

  private[twitter] def parse(line: String): LikeInfo = {
    val dat = line.split("|").toList
    val ts = dat(0).split("+").toList.head.split("T")
    //val date : String = ts(0)
    //val time : String = ts(1)

    val df : DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse(dat(0))
    val timestamp : Timestamp = new Timestamp(date.getTime())
    val user_id : Long = dat(1).toLong
    val comment_id : Long = dat(2).toLong

    LikeInfo(timestamp, user_id, comment_id);
  }
}
