package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming03_DIY {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
        messageDS.print()

        ssc.start()
        ssc.awaitTermination()
    }

    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
        private var flg = true
        override def onStart(): Unit = {
            new Thread(new Runnable {
                override def run(): Unit = {
                    while (flg){
                        val message = "Collected data: " + new Random().nextInt(10).toString
                        store(message)
                        Thread.sleep(500)
                    }
                }
            }).start()
        }

        override def onStop(): Unit = {
            flg = false
        }
    }
}
