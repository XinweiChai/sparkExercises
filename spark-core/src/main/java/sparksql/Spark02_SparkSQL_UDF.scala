package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark02_SparkSQL_UDF {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val df: DataFrame = spark.read.json("data/user.json")
        df.createOrReplaceTempView("user")

        spark.udf.register("prefixName", (name:String)=> {
            "Name: " + name
        })

        spark.sql("select age, prefixName(username) from user").show()

        spark.close()
    }
}
