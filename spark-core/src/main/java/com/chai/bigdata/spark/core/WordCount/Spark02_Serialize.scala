package com.chai.bigdata.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Serialize {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(Array("Hello World", "Hello Scala", "Hive"))
        val search = new Search("H")
        //        search.getMatch1(rdd).collect().foreach(println)
        search.getMatch2(rdd).collect().foreach(println)


        sc.stop()
    }

    class Search(query: String) {
        def isMatch(s: String): Boolean = {
            s.contains(query)
        }

        def getMatch1(rdd: RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }

        def getMatch2(rdd: RDD[String]): RDD[String] = {
            val s: String = query
            rdd.filter(_.contains(s))
        }

    }

}
