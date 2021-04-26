package scalafx

import javafx.event.EventHandler
import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.beans.property.{ObjectProperty, StringProperty}
import scalafx.collections.ObservableBuffer
import scalafx.event.{ActionEvent, EventHandler}
import scalafx.scene.Scene
import scalafx.scene.control.SelectionMode.Single
import scalafx.scene.control.{TableColumn, TableView}

object TableView extends JFXApp {
  case class Student(name: String, test1: Int, test2: Int)

  val data = ObservableBuffer(
    Student("a", 2, 5),
    Student("b", 4, 3),
    Student("c", 5, 6)
  )
  data -= Student("a", 2, 5) // remove Student("a",...

  stage = new JFXApp.PrimaryStage {
    resizable = false
//    onCloseRequest = {
//      println("closing")
//      stage.close()
//    }
    title = "TableView"
    scene = new Scene(300, 200) {

      val table = new TableView(data += (Student("d", 1, 7), Student("e", 3, 4))) {
        editable = false
        selectionModel().selectionMode = Single //Choose between Single and Multiple
        selectionModel.apply.selectedItem.onChange {
          println("Selected" + selectionModel.apply.getSelectedItem)
          println("Selected row-Index: " + selectionModel.apply.selectedIndex.value)
          println("Selected cells: " + selectionModel.apply.selectedCells)
          println("Focused index: " + selectionModel.apply.focusedIndex)
        }
      }

      val col1 = new TableColumn[Student, String]("Name")
      col1.cellValueFactory = cdf => StringProperty(cdf.value.name)

      val col2 = new TableColumn[Student, Int]("Test1")
      col2.cellValueFactory = cdf => ObjectProperty(cdf.value.test1)

      val col3 = new TableColumn[Student, Int]("Test2")
      col3.cellValueFactory = cdf => ObjectProperty(cdf.value.test2)

      val col4 = new TableColumn[Student, Double]("Average")
      col4.cellValueFactory = cdf => ObjectProperty(
        (cdf.value.test1 + cdf.value.test2) / 2.0
      )

      table.columns ++= List(col1, col2, col3, col4)
      root = table
    }
  }

//  stage.onCloseRequest = {
//    println("Stage is closing")
//    stage.close()
//  }
//  stage.setOnCloseRequest(eh: EventHandler) =>  {
//    println("Stage is closing")
//    stage.close()
//  }

}
