package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameter, ParametersDelegate}
import org.locationtech.geomesa.tools.FeaturesTool
import org.locationtech.geomesa.tools.commands.Explain.Command

class Explain(parent: JCommander) {

  val params = new CqlParams()
  parent.addCommand(Command, params)

  // TODO fill in null
  def execute() = new FeaturesTool(null, null).explainQuery()

}

object Explain {
  val Command = "explain"
}
