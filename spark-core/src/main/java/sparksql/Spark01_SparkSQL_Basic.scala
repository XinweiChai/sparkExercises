package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

//        val df: DataFrame = spark.read.json("data/user.json")

//        df.createOrReplaceTempView("user")
//        spark.sql("SELECT * FROM user").show()

//        df.select("age", "username").show()
//        df.select('age + 1).show()

//        val seq = Seq(1,2,3,4)
//        val ds: Dataset[Int] = seq.toDS()
//        ds.show()

        // RDD <=> DF
        val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",40)))
        val df: DataFrame = rdd.toDF("id", "name", "age")
        val rowRDD: RDD[Row] = df.rdd

        // DF <=> DS
        val ds: Dataset[User] = df.as[User]
        val df1: DataFrame = ds.toDF()

        // RDD <=> DS
        val ds1: Dataset[User] = rdd.map {
            case (id, name, age) =>
                User(id, name, age)
        }.toDS()
        val userRDD: RDD[User] = ds.rdd

        spark.close()
    }
    case class User(id:Int, name:String, age:Int)
}
