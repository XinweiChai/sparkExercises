package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_State {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")

        val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

        // In case we want to keep the data
        // 使用有状态操作时，需要设定checkpoint路径
        val wordToOne: DStream[(String, Int)] = data.map((_, 1))

        // seq: 相同key的value数据
        // opt: 缓冲区相同key的value数据
        val state: DStream[(String, Int)] = wordToOne.updateStateByKey(
            (seq: Seq[Int], buff: Option[Int]) => {
                val newCount: Int = buff.getOrElse(0) + seq.sum
                Option(newCount)
            }
        )
        state.print()

        ssc.start()
        ssc.awaitTermination()
    }

}
