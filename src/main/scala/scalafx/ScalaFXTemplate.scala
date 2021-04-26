package scalafx

import scalafx.application.JFXApp
import scalafx.scene.Scene

object ScalaFXTemplate extends JFXApp {
  stage = new JFXApp.PrimaryStage {
    title = "ScalaFX Template"
    scene = new Scene(300, 200) {}
  }

}
