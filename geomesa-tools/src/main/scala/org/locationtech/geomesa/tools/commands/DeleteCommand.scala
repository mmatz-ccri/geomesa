package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{Parameter, ParametersDelegate, JCommander}
import org.locationtech.geomesa.tools.FeaturesTool
import org.locationtech.geomesa.tools.commands.DeleteCommand._

class DeleteCommand(parent: JCommander) {

  val params = new GeoMesaParams
  parent.addCommand(Command, params)

  // TODO fill in null
  def execute() = new FeaturesTool(null, null).deleteFeature()

  class DeleteParams {
    @Parameter(names = Array("--force"), description = "Force deletion of feature without prompt", required = false)
    var forceDelete: Boolean = false

    @ParametersDelegate
    val featureParams = new FeatureParams()
  }
}

object DeleteCommand {
  val Command = "delete"
}
