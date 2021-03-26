package com.chai.bigdata.spark.core.WordCount.framework.controller

import com.chai.bigdata.spark.core.WordCount.framework.common.TController
import com.chai.bigdata.spark.core.WordCount.framework.service.WordCountService



class WordCountController extends TController{
    private val wordCountService = new WordCountService()

    def dispatch(): Unit ={
        val result = wordCountService.dataAnalysis()
        result.foreach(println)
    }
}
