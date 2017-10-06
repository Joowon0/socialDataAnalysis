import java.util.Date

import dataTypes.Post
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
conf.setMaster("local[*]")
conf.setAppName("Simple Application2")

val sc = new SparkContext(conf)

val ts = new dataTypes.Timestamp(new Date())
val a : Post = new Post(1, ts)
val b : Post = new Post(2, ts)

val x : RDD[(Post, Int)] = sc.parallelize(Seq((a,1), (b,2)))
val printTemp = x.groupByKey().collect

printTemp.foreach(println)