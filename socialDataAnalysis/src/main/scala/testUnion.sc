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
val distinct = yz.groupByKey().map{
  case (a, set) => (a, set.flatten.toSet)
}.collect()


distinct.foreach(println)
