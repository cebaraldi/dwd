package gui

import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.event.ActionEvent
import scalafx.scene.Scene
import scalafx.scene.control.{Button, ComboBox, ListView}

object FirstGUI extends JFXApp {
  stage = new JFXApp.PrimaryStage {
    resizable = false
    title = "First GUI"
    scene = new Scene(240, 180) {

      val button = new Button("Click Me!")
      button.layoutX = 20
      button.layoutY = 20

      val comboBox = new ComboBox(
        List("Scala", "Java", "C++", "Haskell"))
      comboBox.layoutX = 125
      comboBox.layoutY = 20

      val listView = new ListView(
        List("AWT", "Swing", "JavaFX", "ScalaFX"))
      listView.layoutX = 20
      listView.layoutY = 60
      listView.prefHeight = 100
      listView.prefWidth = 200

      content = List(button, comboBox, listView)

      button.onAction = (e: ActionEvent) => {
        val selected = listView.selectionModel.apply.getSelectedItems
        listView.items = listView.items.apply.diff(selected)
        println("Button clicked.")
      }

      comboBox.onAction = (e: ActionEvent) => {
        listView.items.apply += comboBox.selectionModel.apply.getSelectedItem
      }

    }
  }
}
