package com.chai.bigdata.spark.core.WordCount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator, LongAccumulator}

import scala.collection.mutable

object Spark03_Accumulator {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        //        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val acc: LongAccumulator = sc.longAccumulator("sum")
        //        rdd.foreach(acc.add(_))
        //        val temp = rdd.map(acc.add(_))
        //        temp.collect()
        //        temp.collect()
        //        println(acc.value)

        val rdd1: RDD[String] = sc.makeRDD(Array("Hello World", "Hello Scala", "Hive"))
        val words: RDD[String] = rdd1.flatMap(_.split(" "))
        val wc = new MyAccumulator()
        sc.register(wc)
        words.foreach {
            word => {
                wc.add(word)
            }
        }
        println(wc.value)
        sc.stop()
    }

    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
        private var wcMap = mutable.Map[String, Long]()

        override def isZero: Boolean = wcMap.isEmpty

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            val acc = new MyAccumulator()
            acc.wcMap = this.wcMap.clone()
            acc
        }

        override def reset(): Unit = wcMap.clear()

        override def add(word: String): Unit = wcMap.update(word, wcMap.getOrElse(word, 0L) + 1)

        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            other.value.foreach {
                case (word, count) =>
                    wcMap.update(word, wcMap.getOrElse(word, 0L) + count)
            }
        }

        override def value: mutable.Map[String, Long] = wcMap
    }

}
