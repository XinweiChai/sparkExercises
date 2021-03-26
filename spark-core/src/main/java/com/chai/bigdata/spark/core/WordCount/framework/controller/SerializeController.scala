package com.chai.bigdata.spark.core.WordCount.framework.controller

import com.chai.bigdata.spark.core.WordCount.framework.common.TController
import com.chai.bigdata.spark.core.WordCount.framework.service.SerializeService

class SerializeController extends TController{
    private val serializeService = new SerializeService()
    def dispatch(): Unit ={
        val result = serializeService.dataAnalysis()
        result.collect().foreach(println)
    }
}
