package dwd

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.text.SimpleDateFormat

case class DWDStation(
                       sid: Int,
                       von: java.sql.Date,
                       bis: java.sql.Date,
                       height: Int,
                       lat: Double,
                       lon: Double,
                       wsname: String,
                       state: String
                     )

object Station extends App {

  def toDate(s: String, sformat: String): java.sql.Date = {
    val dateFormat = new SimpleDateFormat(sformat)
    val date: java.util.Date = dateFormat.parse(s)
    val d: java.sql.Date = new java.sql.Date(date.getTime())
    d
  }

  // Start spark
  val spark = SparkSession.builder().
    master("local[*]").
    appName("DWD Station").
    getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // Load JSON configuration from resources directory
  val cfgJSON = "src/main/resources/cfgXenos_dwd.json"
  val cfg = spark.read.option("multiLine", true)
    .json(cfgJSON).toDF().filter('object === "Station")
  val inPath = cfg.select('inPath).as[String].collect()(0)
  val klStationFile = cfg.select('KL_StationFile).as[String].collect()(0)
  val outPath = cfg.select('outPath).as[String].collect()(0)
  val pqFile = cfg.select('pqFile).as[String].collect()(0)

  val lines = spark.read.textFile(inPath + klStationFile).
    filter(!_.contains("Stations_id")).
    filter(!_.contains("-----------"))

  val rdd = lines.map(line => {
    //    val e = line.split("\\s+") // split a string on whitespace characters, didn't do it ...
    DWDStation(
      line.substring(0, 5).toInt,
      toDate(line.substring(6, 14), "yyyyMMdd"),
      toDate(line.substring(15, 23), "yyyyMMdd"),
      line.substring(24, 38).trim.toInt,
      line.substring(39, 50).trim.toDouble,
      line.substring(51, 60).trim.toDouble,
      line.substring(61, 101).trim,
      line.substring(102, 200).trim)
  })

  // Write a parquet file
  val df = rdd.toDF
  //  import org.apache.spark.sql.functions._ <---- German Umlaute not read in.
  //    'withColumn("stn",encode('wsname,"ISO-8859-1")). or decode ...
  //    withColumn("sta",encode('state,"ISO-8859-1"))
  //  val pqfName = "DWDStation.parquet"
  df.write.mode(SaveMode.Overwrite).parquet(outPath + pqFile)
  df.show(false)

  spark.stop()
}
