package wikipedia

import java.io.File
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.LikeInfo

/**
  * Created by syndr on 2017-08-09.
  */


object LikesData {
  private[wikipedia] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("./data/likes.dat")
    if (resource == null) sys.error("likes.dat == null")
    new File(resource.toURI).getPath
  }

  private[wikipedia] def parse(line: String): LikeInfo = {
    val dat : Array[String]= line.split("\\|")
    val ts = dat(0).split("\\+").toList.head.split("T")

    val df : DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse(dat(0))

    val timestamp : Timestamp = new Timestamp(date.getTime())
    val user_id : Long = dat(1).toLong
    val comment_id : Long = dat(2).toLong
    /*
    println("-------------------------")
    println("timestamp : " + timestamp)
    println("user_id : " + user_id)
    println("comment_id : " + comment_id)
    */
    LikeInfo(timestamp, user_id, comment_id)
  }
}
