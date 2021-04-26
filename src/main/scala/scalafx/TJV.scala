package scalafx

import dwd.DWDStation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.beans.property.{ObjectProperty, StringProperty}
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.control.SelectionMode.{Single}
import scalafx.scene.control.{TableColumn, TableView}

import java.text.SimpleDateFormat

object TJV extends JFXApp {

  val spark = SparkSession.builder().
    master("local[*]").
    appName("TJV").
    getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val dwdPath = "/media/datalake"
  val wsPqf = dwdPath + "/" + "DWDStation.parquet"
  val obsPqf = dwdPath + "/" + "DWDHistoricalData.parquet"
  val df = spark.read.parquet(wsPqf)

  import spark.implicits._

  val lst = df.as[DWDStation].collectAsList
  val data = ObservableBuffer(lst.get(0)) // initialise buffer
  for (i <- 1 until lst.size()) data += lst.get(i) // add remaining items

  stage = new JFXApp.PrimaryStage {
    onCloseRequest = () => {
      //      println("Stage is closing")
      println("Spark is closing")
      spark.stop()
    }
    resizable = false
    title = "German Weather Stations"
    scene = new Scene(835, 800) {
      val table = new TableView(data) {
        editable = false
        selectionModel().selectionMode = Single //Choose between Single and Multiple
        selectionModel.apply.selectedItem.onChange {
          computeBasicStatistics(selectionModel.apply.getSelectedItem)
        }
      }

      val col1 = new TableColumn[DWDStation, String]("state")
      col1.cellValueFactory = cdf => StringProperty(cdf.value.state.trim)
      col1.setSortType(TableColumn.SortType.Ascending)

      val col2 = new TableColumn[DWDStation, String]("ws")
      col2.setSortType(TableColumn.SortType.Ascending)
      col2.cellValueFactory = cdf => StringProperty(cdf.value.wsname.trim)

      val col3 = new TableColumn[DWDStation, Int]("sid")
      col3.cellValueFactory = cdf => ObjectProperty(cdf.value.sid)
      col3.prefWidth = 50

      val col4 = new TableColumn[DWDStation, Double]("lat")
      col4.cellValueFactory = cdf => ObjectProperty(cdf.value.lat)

      val col5 = new TableColumn[DWDStation, Double]("lon")
      col5.cellValueFactory = cdf => ObjectProperty(cdf.value.lon)

      val col6 = new TableColumn[DWDStation, Double]("height")
      col6.cellValueFactory = cdf => ObjectProperty(cdf.value.height)

      val col7 = new TableColumn[DWDStation, java.sql.Date]("von")
      col7.cellValueFactory = cdf => ObjectProperty(cdf.value.von)

      val col8 = new TableColumn[DWDStation, java.sql.Date]("bis")
      col8.cellValueFactory = cdf => ObjectProperty(cdf.value.bis)

      table.columns ++= List(col1, col2, col7, col8, col6, col4, col5, col3)

      // Sorting given above SortType
      table.getSortOrder.add(col1)
      table.getSortOrder.add(col2)
      table.sort()

      root = table
    }
  }

  def toDate(s: String, sformat: String): java.sql.Date = {
    val dateFormat = new SimpleDateFormat(sformat)
    val date: java.util.Date = dateFormat.parse(s)
    val d: java.sql.Date = new java.sql.Date(date.getTime())
    d
  }

  def computeBasicStatistics(dwd: DWDStation): Unit = {
    println(s"Compute Basic Statistics of Weather Station ${dwd.sid}: ${dwd.wsname}")
    val data = spark.read.parquet(obsPqf).filter('sid === dwd.sid)
    val bsWeatherStation = data.agg(
      avg('tmk) as "tavg",
      variance('tmk) as "tvar",
      skewness('tmk) as "tskewness",
      kurtosis('tmk) as "tkurtosis",
      count('tmk) as "tcount"
    )
    bsWeatherStation.show(false)
  }
}
