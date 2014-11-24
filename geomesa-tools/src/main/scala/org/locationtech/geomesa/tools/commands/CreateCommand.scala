package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.FeatureCreator
import org.locationtech.geomesa.tools.commands.CreateCommand.Command

class CreateCommand(parent: JCommander) extends Command with Logging {

  val params = new CreateParams()
  parent.addCommand(Command, params)

  override def execute() = FeatureCreator.createFeature(params)
}

object CreateCommand {
  val Command = "create"
}