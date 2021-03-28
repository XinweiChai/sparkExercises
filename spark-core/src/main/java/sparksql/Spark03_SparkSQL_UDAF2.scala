package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator


object Spark03_SparkSQL_UDAF2 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val df: DataFrame = spark.read.json("data/user.json")
        df.createOrReplaceTempView("user")

        // 早期版本中，Spark不能在sql中使用强类型UDAF
        val ds: Dataset[User] = df.as[User]

        val udafColumn: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

        ds.select(udafColumn).show()

        spark.close()
    }

    // org.apache.spark.sql.expressions.Aggregator
    // IN: Input BUF: OUT: Output

    case class User(username:String, age: Long)
    case class Buff(var total: Long, var count: Long)

    class MyAvgUDAF extends Aggregator[User, Buff, Long] {

        // Initialized value
        override def zero: Buff = {
            Buff(0L, 0L)
        }

        override def reduce(b: Buff, a: User): Buff = {
            b.total += a.age
            b.count = b.count + 1
            b
        }

        override def merge(b1: Buff, b2: Buff): Buff = {
            b1.total += b2.total
            b1.count += b2.count
            b1
        }

        override def finish(reduction: Buff): Long = {
            reduction.total / reduction.count
        }

        override def bufferEncoder: Encoder[Buff] = Encoders.product

        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }

}
