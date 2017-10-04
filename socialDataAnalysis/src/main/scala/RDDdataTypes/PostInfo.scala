package RDDdataTypes

import java.sql.Timestamp

case class PostInfo(timestamp : Timestamp, post_id : Long,
                    user_id : Long, post : String, user : String) {}