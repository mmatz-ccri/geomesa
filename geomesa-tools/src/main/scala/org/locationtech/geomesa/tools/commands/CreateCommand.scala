package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.commands.CreateCommand.Command

class CreateCommand(parent: JCommander) extends Logging {

  val params = new GeoMesaParams()
  parent.addCommand(Command, params)

  //TODO implement
  def execute() = {

  }

}

object CreateCommand {
  val Command = "create"
}