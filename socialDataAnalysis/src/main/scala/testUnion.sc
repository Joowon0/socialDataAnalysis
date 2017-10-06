import java.util.Date

import dataTypes.Post
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
conf.setMaster("local[*]")
conf.setAppName("Simple Application")

val sc = new SparkContext(conf)


//val a: Map[Int, Option[Iterable[Int]]] = Map(10 -> Some(Iterable(1,2,3)), 11 -> Some(Iterable(4,5,6)), 12 -> None)
val b: Map[Int, Option[Iterable[Int]]] = Map(10 -> Some(Iterable(3,4,5)), 11 -> None, 12 -> None)
val c: Map[Int, Set[Int]] = Map(10 -> Set(1,2,3), 11 -> Set(4,5,6), 12 -> Set())

//val x: RDD[(Int, Option[Iterable[Int]])] = sc.parallelize(a.toSeq)
val y: RDD[(Int, Option[Iterable[Int]])] = sc.parallelize(b.toSeq)
val z: RDD[(Int, Set[Int])] = sc.parallelize(c.toSeq)

/*val xy = (x union y).groupByKey().map{
  case (a, list) => (a, list.flatten.flatten.toSet)
}.collect()

xy.foreach(println)*/

val yz : RDD[(Int, Set[Int])] = y.map {
  case (a, list) => (a, list.toSet.flatten)
} union z
val distinct = yz.groupBy {
  case(key, value) => key
}.map{
  case (postID, set) =>
    val comments = set.flatMap {
    case(postID, eachSet) => eachSet
  }
    (set.head._1, comments)
}.collect()


distinct.foreach(println)

/*
val ts = new dataTypes.Timestamp(new Date())
val a : Post = new Post(1, ts)
val b : Post = new Post(1, ts)

println(a == b)

val x : RDD[(Post, Int)] = sc.parallelize(Seq((a,1), (a,2)))
val printTemp = x.groupBy{case (p,i) => p.PostID}.collect

printTemp.foreach(println)*/
