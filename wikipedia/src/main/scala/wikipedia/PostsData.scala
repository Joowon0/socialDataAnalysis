package wikipedia

import java.io.File
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

/**
  * Created by syndr on 2017-08-09.
  */
object PostsData {
  private[wikipedia] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("./data/posts.dat")
    if (resource == null) sys.error("posts.dat == null")
    new File(resource.toURI).getPath
  }

  private[wikipedia] def parse(line: String): PostInfo = {
    val dat : Array[String]= line.split("\\|")
    val ts = dat(0).split("\\+").toList.head.split("T")

    val df : DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse(dat(0))

    val timestamp : Timestamp = new Timestamp(date.getTime())
    val post_id : Long = dat(1).toLong
    val user_id : Long = dat(2).toLong
    val post : String = dat(3)
    val user : String = dat(4)
    /*
    println("-------------------------")
    println("timestamp : " + timestamp)
    println("post_id : " + post_id)
    println("user_id : " + user_id)
    println("post : " + post)
    println("user : " + user)
    */
    PostInfo(timestamp, post_id, user_id, post, user)
  }
}
