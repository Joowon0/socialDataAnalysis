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

object friendshipData {
  private[friendship] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("twitter/friendships.dat")
    if(resource == null) sys.error("friendships.dat == null")
    new File(resource.toURI).getPath
  }
}

object likeData {
  private[like] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("twitter/like.dat")
    if(resource == null) sys.error("like.dat == null")
    new File(resource.toURI).getPath
  }
}

object postData {
  private[postt] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("twitter/post.dat")
    if(resource == null) sys.error("post.dat == null")
    new File(resource.toURI).getPath
  }
}