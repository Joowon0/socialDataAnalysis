package RDDdataTypes

import java.sql.Timestamp

case class FriendshipInfo(timestamp : Timestamp, user_id_1 : Long,
                          user_id_2 : Long) {}