package com.chai.bigdata.spark.core.WordCount

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Broadcast {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        //        val rdd2 = sc.makeRDD(List(("a",4),("b",5),("c",6)))
        val mp: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
        val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(mp)
        val rdd3: RDD[(String, (Int, Int))] = rdd1.map {
            case (w, c) =>
                val l: Int = bc.value.getOrElse(w, 0)
                (w, (c, l))
        }
        rdd3.collect().foreach(println)

        sc.stop()
    }
}
