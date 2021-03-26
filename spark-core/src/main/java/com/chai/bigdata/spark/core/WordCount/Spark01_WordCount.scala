package com.chai.bigdata.spark.core.WordCount

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

object Spark01_WordCount {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)


        val rdd = sc.textFile("data")
        println(rdd.dependencies)
        val words = rdd.flatMap(_.split(" "))
        println(words.dependencies)
        val wordMap = words.map(
            word => {
                mutable.Map[String, Long]((word, 1))
            }
        )
        println(wordMap.dependencies)
        wordMap.reduce(
            (map1, map2) => {
                map2.foreach {
                    case (word, count) =>
                        val newCount: Long = map1.getOrElse(word, 0L) + count
                        map1.update(word, newCount)
                }
                map1
            }
        ).foreach(println)
        sc.stop()
    }
}
