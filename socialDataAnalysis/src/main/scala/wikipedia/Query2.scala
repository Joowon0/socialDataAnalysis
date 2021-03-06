package wikipedia

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import RDDdataTypes.{CommentInfo, FriendshipInfo, LikeInfo, PostInfo}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Query2 {

  def q2(args: Array[String]) {
//    val conf: SparkConf = new SparkConf().setMaster("spark://192.168.0.195:7077").setAppName("twitterAnalysis")
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis")
    val sc: SparkContext = new SparkContext(conf)

    val timing = new StringBuffer
    def timed[T](label: String, code: => T): T = {
      val start = System.currentTimeMillis()
      val result = code
      val stop = System.currentTimeMillis()
      timing.append(s"Processing $label took ${stop - start} ms.\n")
      result
    }

    val df : DateFormat = new SimpleDateFormat("yyyy-MM-DD'T'HH:mm:ss.SSSSSX")
    val date : Date = df.parse("2010-01-01T03:00:00.000+0000") // 12시 정오임

    //2010-02-01T12:00:00.000
    var currentDate : Timestamp = new Timestamp(date.getTime()) // java.util.Date

    var i = 0
    /** Don't know why start at Jan 01 */
    while (i < 29) {
      i = i + 1
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24)
      currentDate = new Timestamp(date.getTime())
    }
    val date2: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 36)
    currentDate = new Timestamp(date2.getTime())

    /** calling recursive function */
    main_recur(1, 5, 6 * 60 * 60, sc.emptyRDD, List(new dataTypes.Timestamp(date2)))

    /**
      * @param i               - count the number of recursion
      * @param k               - number of post we want to get
      * @param d               - within k seconds
      * @param Friendships     - User ID  -> Set of user id
      * @param daysTimestamp   - all of timestamps
      */
    def main_recur(i : Int, k: Int, d: Int, Friendships : RDD[(Long, Long)], daysTimestamp : List[dataTypes.Timestamp] ) {
      if (i > 35) return

      /** RDD read from file */
      val CommentsRDD: RDD[CommentInfo] = sc.textFile("/home/ana/data/data_day_big10/comments/comments" + i + ".dat").map(CommentsData.parse)
      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("/home/ana/data/data_day_big10/friendships/friendships" + i + ".dat").map(FriendshipsData.parse)
      val LikesRDD: RDD[LikeInfo] = sc.textFile("/home/ana/data/data_day_big10/likes/likes" + i + ".dat").map(LikesData.parse)
//      val CommentsRDD: RDD[CommentInfo] = sc.textFile("src/main/scala/SampleData/commentsSample.dat").map(CommentsData.parse)
//      val FriendshipsRDD: RDD[FriendshipInfo] = sc.textFile("src/main/scala/SampleData/friendshipsSample.dat").map(FriendshipsData.parse)
//      val LikesRDD: RDD[LikeInfo] = sc.textFile("src/main/scala/SampleData/likesSample.dat").map(LikesData.parse)

      // print test
//      println("현재 날짜    : " + currentDate.toString)
//      val printTemp: String = (daysTimestamp map (x => x.toString)).mkString(" ")
//      println("timestamps : " + printTemp)


      /** Comments */
      val newCommentsPair       : RDD[(Long, Long)] = CommentsRDD.map(c=> (c.comment_id, c.comment_id))

      /** Likes */
      val likesCommentUser      : RDD[(Long, Long)] = LikesRDD.map(c => (c.comment_id, c.user_id))
      val likesCommentUserGroup : RDD[(Long, Iterable[Long])] = likesCommentUser groupByKey
      val likesCommentUserNil   : RDD[(Long, Iterable[Long])] =
        ((newCommentsPair leftOuterJoin likesCommentUserGroup) values) mapValues {
          case None => Iterable()
          case Some(x) => x}

//      val printTemp111 = likesCommentUserNil.collect.map {case (cid, users) => "CComment : " + cid + "\tUsers" + users.toString} mkString("\n")
//      println(printTemp111)

      /** users that liked to new comments */
      val allUsers     : RDD[Long] = likesCommentUser map (_._2) distinct
      val allUsersPair : RDD[(Long, Long)] = allUsers map (u => (u,u))
      // print for test
//      println("All Users :")
//      println(allUsers collect() mkString " ")

      // print for test
//      val printTemp1 : String = allUsers.collect.map { case (u) => "User " + u } mkString ("\n")
//      println(printTemp1)
//      val printTemp2 : String = likesCommentUserNil.collect.map {  case (c, us) => "Comment " + c + " : " + us.map(f => "user" + f + ", ")} mkString ("\n")
//      println(printTemp2)

      /** combine old and new friendships  */
      val newFriendships    : RDD[(Long, Long)] = FriendshipsRDD.map(f => (f.user_id_1, f.user_id_2))
      val allFriendships    : RDD[(Long, Long)] = Friendships union newFriendships
//      println("All friendships")
//      val printT3 = allFriendships.collect.map{ case(u1, u2) => "User1 : " + u1 + "\tUser2 : " + u2} mkString ("\n")
//      println(printT3)

      /** filter out unnecessary */
      val useFriendships    : RDD[(Long, Long)] = allFriendships join allUsersPair values
      val useFriendshipsRev : RDD[(Long, Long)] = allUsersPair join allFriendships values

//      println("Friend case 1")
//      val printT1 : String = useFriendships.collect.map{ case(u1, u2) => "User1 : " + u1 + "\tUser2 : " + u2} mkString ("\n")
//      println(printT1)
//      println("Friend case 2 (reverse)")

//      val printT2 : String = useFriendshipsRev.collect.map{ case(u1, u2) => "User1 : " + u1 + "\tUser2 : " + u2} mkString ("\n")
//      println(printT2)

      val useFriendshipsPair : RDD[((Long, Long), (Long, Long))] = useFriendships map (c => (c,c))
      val useFriendshipsRevPair : RDD[((Long, Long), (Long, Long))] = useFriendshipsRev map {case (c1, c2) => ((c2, c1), (c2, c1))}
      val useFriendshipsAll : RDD[(Long, Long)] = useFriendshipsPair join useFriendshipsRevPair keys

      // print for test
//      println("Using Friendships")
//      val printTemp6 : String = useFriendshipsAll.collect.map { case (u1, u2 ) => "User1 : " + u1 + "\tUser2 : " + u2} mkString ("\n")
//      println(printTemp6)

      /** making into graph */
      val friendships : Array[(Long, Long)] = useFriendshipsAll.collect()
      val commentFriendships : RDD[(Long, Iterable[(Long, Long)])] = likesCommentUserNil mapValues
        ( us => friendships filter { case (u1, u2) => (us exists (_ == u1)) && (us exists (_ == u2))})


      val initialGraph : RDD[(Long, Iterable[Iterable[Long]])] = likesCommentUserNil mapValues (u => u map (Iterable(_)))
      val commentLikeFriendship : RDD[(Long, (Iterable[Iterable[Long]], Iterable[(Long, Long)]))] = initialGraph join commentFriendships
//      println("initial graph")
//      val printTemp10 :String = initialGraph.collect.map{
//        case(cid, initial) => "Comment : " + cid + "\tUser : " + initial.map(_.toString)} mkString ("\n")
//      println(printTemp10)

//      println("connections")
//      val printTemp11 : String = commentFriendships.collect.map{
//        case(cid, fr) => "Comment : " + cid + "\tFri : " + fr.map(_.toString()) } mkString ("\n")
//      println(printTemp11)

      def graphRec (vertex : Set[Set[Long]], edges : Iterable[(Long, Long)]): Set[Set[Long]] = {
        if (edges isEmpty) vertex
        else {
          val e = edges.head

//          println(vertex)
          val graph1 : Set[Long] = vertex find (_.contains(e._1)) get
          val graph2 : Set[Long] = vertex find (_.contains(e._2)) get

//          println (e._1 + " : " + graph1)
//          println (e._2 + " : " + graph2)

          if (graph1 == graph2) {
//            println("Remain Same")
//            println(vertex + "\n")
            graphRec(vertex, edges.tail)
          }
          else {
            val rmGraph = vertex - graph1 - graph2
            val mkGraph = rmGraph + (graph1 union graph2)
//            println(mkGraph + "\n")

            graphRec(mkGraph, edges.tail)
          }
        }
      }

      val refinedType : RDD[(Long, (Set[Set[Long]], List[(Long, Long)]))] =
        commentLikeFriendship mapValues {case(v, e) => (v.map(_.toSet).toSet, e.toList) }


      val graph       : Array[(Long, Set[Set[Long]])] = (refinedType collect) map {
        case(c, (v, e)) =>
          if (v.isEmpty) (c,Set(Set())) : ((Long, Set[Set[Long]]))
          else (c,graphRec(v, e))
      }

      /** calculate the max size of graph */
      val graphSize   : Array[(Long, Int)] = graph map {
        case (c, vs) =>
          val sizes = vs.map(_.size)
          val maxSize =
            if (sizes isEmpty) 0 else sizes.max
          (c, maxSize)
      }
      val graphSizeSorted : RDD[(Long, Int)] = sc.parallelize(graphSize) sortBy (_._2, false)
      val extractedTop : Array[(Long, Int)] =
        if( graphSizeSorted.count() > k) graphSizeSorted.take(k)
        else graphSizeSorted.collect()
      //extractedTop foreach (println)

      /** write to file */
      val mapGraph : Map[Long, Set[Set[Long]]] = graph.toMap
      val resultFile : String = extractedTop.map {
        case (cID, size) =>
          val comment : CommentInfo = CommentsRDD.filter{c => c.comment_id == cID} . first()
          val vertex : Set[Set[Long]] = mapGraph(cID)
          val edge : Iterable[(Long, Long)] =commentFriendships.lookup(cID).head

          val size_ = "Size : " + size + "\n"
          val comm_ = comment + "\n"
          val vert_ = "(name, group) :\n" + ((vertex zip (1 to vertex.size)) mkString " ") + "\n"
          val edge_ = "(source, target) : \n" + (edge mkString " ") + "\n"
          size_ + comm_ + vert_ + edge_
      } mkString "\n"

//      val pw = new PrintWriter(new File("/home/ana/data/query2Out/" + i + ".dat" ))
      val pw = new PrintWriter(new File("src/main/scala/query2Out/" + i + ".dat" ))
      pw.write(resultFile )
      pw.close

      // print for test
//      val printTemp4  : String = graphSize map { case (c, s) => "Comment : " + c + "\tSize : " + s} mkString ("\n")
//      println(printTemp4)

      /** processes regards to date */
      val date: Date = new Date(currentDate.getTime() + 1000 * 60 * 60 * 24) // 하루 지남
      currentDate = new Timestamp(date.getTime())
      daysTimestamp map (_.decrease()) // decrease the scores of the old dates
      val filteredTS = daysTimestamp filter (ts => (ts.score > 0))

      main_recur( i+1, k, d, allFriendships, new dataTypes.Timestamp(date) :: filteredTS)
    }

    println(timing)
    sc.stop()
  }
}