package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.FeaturesTool
import org.locationtech.geomesa.tools.commands.DescribeCommand._

class DescribeCommand(parent: JCommander) {

  val params = new GeoMesaParams
  parent.addCommand(Command, params)

  // TODO fill in null
  def execute() = new FeaturesTool(null, null).describeFeature()
}

object DescribeCommand {
  val Command = "describe"
}

