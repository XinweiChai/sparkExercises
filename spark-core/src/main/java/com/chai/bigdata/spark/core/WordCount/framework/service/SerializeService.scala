package com.chai.bigdata.spark.core.WordCount.framework.service

import com.chai.bigdata.spark.core.WordCount.framework.common.TService
import com.chai.bigdata.spark.core.WordCount.framework.dao.SerializeDao
import org.apache.spark.rdd.RDD

class SerializeService extends TService {
    private val serializeDao = new SerializeDao()

    def dataAnalysis() = {
        val rdd = serializeDao.readFile("data")
        val words = rdd.flatMap(_.split(" "))
        val search = new Search("H")
        //        search.getMatch1(rdd).collect().foreach(println)
        search.getMatch2(words)
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
