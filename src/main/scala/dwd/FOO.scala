package dwd

import org.apache.spark.sql.SparkSession

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}

object FOO extends App {
  val spark = SparkSession.builder().
    master("local[*]").
    appName("DWD Station").
    getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val filename = "/home/sprk/tmp/KL.txt"
  val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)
  //  Source.fromFile(filename)(decoder).getLines().toList
  val df = Source.fromFile(filename)(decoder).getLines().toList
  for (i <- df) println(i)
  println("done")
  spark.stop()
}
