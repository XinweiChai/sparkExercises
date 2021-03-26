package com.chai.bigdata.spark.core.WordCount.framework.service

import com.chai.bigdata.spark.core.WordCount.framework.common.TService
import com.chai.bigdata.spark.core.WordCount.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class WordCountService extends TService{
    private val wordCountDao = new WordCountDao()

    def dataAnalysis(): mutable.Map[String, Long] ={
        val rdd = wordCountDao.readFile("data")
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordMap: RDD[mutable.Map[String, Long]] = words.map(
            word => {
                mutable.Map[String, Long]((word, 1))
            }
        )
        val result: mutable.Map[String, Long] = wordMap.reduce(
            (map1, map2) => {
                map2.foreach {
                    case (word, count) =>
                        val newCount: Long = map1.getOrElse(word, 0L) + count
                        map1.update(word, newCount)
                }
                map1
            }
        )
        result
    }
}
