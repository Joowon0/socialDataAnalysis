package wikipedia

import java.io.File
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

/**
  * Created by syndr on 2017-08-09.
  */
object FriendshipsData {
  private[twitter] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("data/friendships.dat")
    if (resource == null) sys.error("friendships.dat == null")
    new File(resource.toURI).getPath
  }

  private[twitter] def parse(line: String): FriendshipInfo = {
    val dat = line.split("|").toList
    val ts = dat(0).split("+").toList.head.split("T")
    //val date : String = ts(0)
    //val time : String = ts(1)

    val df : DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse(dat(0))
    val timestamp : Timestamp = new Timestamp(date.getTime())
    val user_id_1 : Long = dat(1).toLong
    val user_id_2 : Long = dat(2).toLong

    FriendshipInfo(timestamp, user_id_1, user_id_2)
  }
}
