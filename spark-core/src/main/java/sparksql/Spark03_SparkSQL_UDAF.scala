package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object Spark03_SparkSQL_UDAF {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val df: DataFrame = spark.read.json("data/user.json")
        df.createOrReplaceTempView("user")

        spark.udf.register("ageAvg", new MyAvgUDAF)

        spark.sql("select ageAvg(age) from user").show()

        spark.close()
    }

    class MyAvgUDAF extends UserDefinedAggregateFunction {


        override def inputSchema: StructType = {
            StructType(Array(StructField("age", LongType)))
        }

        override def bufferSchema: StructType = {
            StructType(Array(StructField("total", LongType), StructField("count", LongType)))
        }

        // Output datatype
        override def dataType: DataType = LongType

        // Whether the result is deterministic
        override def deterministic: Boolean = true

        // Initialize buffer
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = 0L
        }

        // Update buffer
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer.update(0, buffer.getLong(0) + input.getLong(0))
            buffer.update(1, buffer.getLong(1) + 1)
        }

        // Merge buffers among partitions
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }

        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0) / buffer.getLong(1)
        }
    }

}
