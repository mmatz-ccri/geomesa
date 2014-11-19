package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander
import ListCommand._
import org.locationtech.geomesa.tools.FeaturesTool

class ListCommand(parent: JCommander) {

  val params = new GeoMesaParams
  parent.addCommand(Command, params)

  // TODO enable
  def execute() = {
    println("list")
    //new FeaturesTool(null, null).listFeatures()
  }
}

object ListCommand {
  val Command = "list"
}
