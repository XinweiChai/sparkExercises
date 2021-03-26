package com.chai.bigdata.spark.core.WordCount.framework.application

import com.chai.bigdata.spark.core.WordCount.framework.common.TApplication
import com.chai.bigdata.spark.core.WordCount.framework.controller.WordCountController

object WordCount extends App with TApplication{
    start("local[*]","WordCount"){
        val controller = new WordCountController()
        controller.dispatch()
    }

}
