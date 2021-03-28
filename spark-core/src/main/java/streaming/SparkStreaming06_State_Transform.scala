package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        // 可以将RDD获取后进行操作
        // Driver端
        val newDS: DStream[String] = lines.transform(
            rdd =>{
                // Driver端，周期性执行
                rdd.map{
                    str=>{
                        // Executor端
                        str
                    }
                }
            }
        )




        ssc.start()
        ssc.awaitTermination()
    }

}
