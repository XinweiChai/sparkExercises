package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_Queue {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val rddQueue = new mutable.Queue[RDD[Int]]()

        val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)
        val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
        mappedStream.reduceByKey(_ + _).print()

        ssc.start()
        for (_ <-1 to 5){
            rddQueue+=ssc.sparkContext.makeRDD(1 to 300,10)
            Thread.sleep(2000)
        }
        ssc.awaitTermination()
    }
}
