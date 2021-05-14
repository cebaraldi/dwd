package dwd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.{FileInputStream, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.zip.ZipInputStream
import scala.swing.FileChooser

case class DWDData(
                    sid: Int,
                    date: java.sql.Date,
                    qn_3: Option[Int], fx: Option[Double], fm: Option[Double],
                    qn_4: Option[Int], rsk: Option[Double], rskf: Option[Double], sdk: Option[Double],
                    shk_tag: Option[Double], nm: Option[Double], vpm: Option[Double], pm: Option[Double],
                    tmk: Option[Double], upm: Option[Double], txk: Option[Double], tnk: Option[Double],
                    tgk: Option[Double]
                  )

object Data extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val  dtFormat =  DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")

  def toDate(s: String, sformat: String): java.sql.Date = {
    val dateFormat = new SimpleDateFormat(sformat)
    val date: java.util.Date = dateFormat.parse(s)
    val d: java.sql.Date = new java.sql.Date(date.getTime())
    d
  }

  def toIntOrNull(s: String): Option[Int] = {
    try {
      if (s == "-999") None else Some(s.trim.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def toDoubleOrNull(s: String): Option[Double] = {
    try {
      if (s == "-999") None else Some(s.trim.toDouble)
    } catch {
      case e: Exception => None
    }
  }

  def zipExtractData(tageswerteKL: String, dwdTemporaryFile: String): Unit = {
    // Extract data file like "produkt_klima_tag_" from zip file to dwdTemporaryFile
    val fis = new FileInputStream(tageswerteKL)
    val zis = new ZipInputStream(fis)
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (file.getName.startsWith("produkt_klima_tag_")) {
        val fout = new FileOutputStream(dwdTemporaryFile)
        val buffer = new Array[Byte](1024)
        Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(fout.write(buffer, 0, _))
      }
    }
    zis.close
    fis.close
  }

  def rdd2Dataset(dwd: String): Dataset[DWDData] = {
    import spark.implicits._
    val lines = spark.read.textFile(dwd).filter(!_.contains("STATIONS_ID;"))
    val ds = lines.map(line => {
      val p = line.split(";")
      DWDData(
        p(0).trim.toInt, toDate(p(1), "yyyyMMdd"),
        toIntOrNull(p(2)), toDoubleOrNull(p(3)), toDoubleOrNull(p(4)),
        toIntOrNull(p(5)), toDoubleOrNull(p(6)), toDoubleOrNull(p(7)), toDoubleOrNull(p(8)),
        toDoubleOrNull(p(9)), toDoubleOrNull(p(10)), toDoubleOrNull(p(11)), toDoubleOrNull(p(12)),
        toDoubleOrNull(p(13)), toDoubleOrNull(p(14)), toDoubleOrNull(p(15)), toDoubleOrNull(p(16)), toDoubleOrNull(p(17))
      )
    }).as[DWDData]
    ds
  }

  // Start spark
  val spark = SparkSession.builder().
    master("local[*]").
    appName("DWD Data").
    getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // Load JSON configuration from resources directory
  val cfgJSON = "src/main/resources/cfgXenos_dwd.json"
  val cfg = spark.read.option("multiLine", true)
    .json(cfgJSON).toDF().filter(trim('object) === "Data")
  val outPath = cfg.select('outPath).as[String].collect()(0)
  val pqFile = cfg.select('pqFile).as[String].collect()(0)

  // File select GUI
  val chooser = new FileChooser
  chooser.multiSelectionEnabled = true
  chooser.title = "Select DWD files"
  if (chooser.showOpenDialog(null) == FileChooser.Result.Approve) {
    val n = chooser.selectedFiles.size
    println(s"${LocalDateTime.now().format(dtFormat)} INFO: $n Files selected")

    val opqFile = outPath + pqFile
    spark.time {
      // Extract data file like "produkt_klima_tag_" from zip file to variable temporary file dwd
      val dwd = "src/main/resources/foobar"
      zipExtractData(chooser.selectedFiles(0).getAbsolutePath, dwd)
      val dwdDS = rdd2Dataset(dwd)
      dwdDS.write.mode(SaveMode.Overwrite).parquet(opqFile)

      if (n > 1) {
        for (i <- 1 until n) {
          zipExtractData(chooser.selectedFiles(i).getAbsolutePath, dwd)
          val dwdDS = rdd2Dataset(dwd)
          dwdDS.write.mode(SaveMode.Append).parquet(opqFile)
        } // for
      } // if
      spark.stop()
    } // spark timer
  } // if chooser

}
