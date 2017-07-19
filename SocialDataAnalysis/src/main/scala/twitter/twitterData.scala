package twitter

import java.io.File

/**
  * Created by syndr on 2017-07-19.
  */
object commentData {
  private[comment] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("twitter/comments.dat")
    if(resource == null) sys.error("comments.dat == null")
    new File(resource.toURI).getPath
  }
}
