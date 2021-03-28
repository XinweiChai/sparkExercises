package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator


object Spark04_SparkSQL_JDBC {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val df = spark.read.format("jdbc").
          option("url","jdbc:mysql://xxxx:3306/yyyy").
          option("driver","com.mysql.jdbc.Driver").
          option("user","root").
          option("password","123123").
          option("dbtable","user").
          load()

        df.show()

        df.write.format("jdbc")
          .option("url","jdbc:mysql://xxxx:3306/yyyy")
          .option("driver","com.mysql.jdbc.Driver")
          .option("user","root")
          .option("password","123123")
          .option("dbtable","user1")
          .mode(SaveMode.Append)
          .save()

        df.write.save()
        spark.close()
    }

}
