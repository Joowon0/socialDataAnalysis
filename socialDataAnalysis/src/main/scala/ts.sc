import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

val df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
val date : Date = df.parse("2010-01-01T03:00:00.000+0000") // 12시 정오임

//2010-02-01T12:00:00.000
val plus : Long = 1000 * 60 * 60 * 24 * 21
var currentDate : Timestamp = new Timestamp(date.getTime()+ plus) // java.util.Date
currentDate.toString