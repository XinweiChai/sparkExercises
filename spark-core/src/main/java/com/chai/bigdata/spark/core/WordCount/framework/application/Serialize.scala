package com.chai.bigdata.spark.core.WordCount.framework.application

import com.chai.bigdata.spark.core.WordCount.framework.common.TApplication
import com.chai.bigdata.spark.core.WordCount.framework.controller.SerializeController

object Serialize extends App with TApplication{
    start("local[*]","Serialize"){
        val controller = new SerializeController()
        controller.dispatch()
    }
}