package wikipedia

import java.io.File
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.FriendshipInfo

/**
  * Created by syndr on 2017-08-09.
  */


object FriendshipsData {
  private[wikipedia] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("./data/friendships.dat")
    if (resource == null) sys.error("friendships.dat == null")
    new File(resource.toURI).getPath
  }

  private[wikipedia] def parse(line: String): FriendshipInfo = {
    val dat : Array[String]= line.split("\\|")//.toList
    val ts = dat(0).split("\\+").toList.head.split("T")

    val df : DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse(dat(0))

    val timestamp : Timestamp = new Timestamp(date.getTime())
    val user_id_1 : Long = dat(1).toLong
    val user_id_2 : Long = dat(2).toLong
    //println("timestamp : " + timestamp)
    /*
    println("-------------------------")
    println("timestamp : " + timestamp)
    println("user_id_1 : " + user_id_1)
    println("user_id_2 : " + user_id_2)
    */
    FriendshipInfo(timestamp, user_id_1, user_id_2)
  }
}
