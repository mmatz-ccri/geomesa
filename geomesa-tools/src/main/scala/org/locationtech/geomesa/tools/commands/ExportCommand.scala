package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameter, ParametersDelegate}
import org.locationtech.geomesa.tools.FeaturesTool
import org.locationtech.geomesa.tools.commands.ExportCommand.Command

class ExportCommand(parent: JCommander) {

  val params = new ExportParams
  parent.addCommand(Command, params)

  //TODO implement
  def execute() = {
    println("geomesa exporter")
  }

  class ExportParams extends CqlParams {
    @Parameter(names = Array("--format"), description = "Format to export (csv|tsv|gml|json|shp)", required = true)
    var format: String = null

    @Parameter(names = Array("--maxFeatures", "-m"), description = "Maximum number of features to return. default: Long.MaxValue")
    var maxFeatures: Long = Long.MaxValue

    @Parameter(names = Array("--stdout", "-s"), description = "flag to force export to std out")
    var stdOut: Boolean = false

    @Parameter(names = Array("--attributes", "-a"), description = "Attributes from feature to export " +
      "(comma-separated)...Comma-separated expressions with each in the format " +
      "attribute[=filter_function_expression]|derived-attribute=filter_function_expression. " +
      "filter_function_expression is an expression of filter function applied to attributes, literals " +
      "and filter functions, i.e. can be nested")
    var attributes: String = null

    @Parameter(names = Array("--idAttribute"), description = "name of the id attribute to export")
    var idAttribute: String = null

    @Parameter(names = Array("--latAttribute"), description = "name of the latitude attribute to export")
    var latAttribute: String = null

    @Parameter(names = Array("--lonAttribute"), description = "name of the longitude attribute to export")
    var lonAttribute: String = null

    @Parameter(names = Array("--dateAttribute"), description = "name of the date attribute to export")
    var dateAttribute: String = null
  }
}

object ExportCommand {
  val Command = "export"
}
