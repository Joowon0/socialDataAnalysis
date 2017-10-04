import org.apache.spark.{SparkConf, SparkContext}

val a : Map[Int, Char] = Map((1 -> 'a'), (2 -> 'b'))

a + (2 -> 'c')

val conf: SparkConf = new SparkConf().setMaster("local").setAppName("twitterAnalysis")
val sc: SparkContext = new SparkContext(conf)

val b = sc.parallelize(a.toSeq())
