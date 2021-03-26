package com.chai.bigdata.spark.core.WordCount.framework.common

import com.chai.bigdata.spark.core.WordCount.framework.util.EnvUtil

trait TDao {
    def readFile(path: String) = {
        EnvUtil.take().textFile(path)
    }
}
