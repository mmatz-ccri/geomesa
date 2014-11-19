package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameter, Parameters, ParametersDelegate}
import org.locationtech.geomesa.tools.TableTools
import org.locationtech.geomesa.tools.commands.Runner.mkSubCommand
import org.locationtech.geomesa.tools.commands.TableConfCommand._

class TableConfCommand(parent: JCommander) {

  val jcTableConf = mkSubCommand(parent, Command, new TableConfParams())
  val tcListParams   = new ListParams
  val tcUpdateParams = new UpdateParams
  val tcDescParams   = new DescribeParams

  mkSubCommand(jcTableConf, ListSubCommand, tcListParams)
  mkSubCommand(jcTableConf, DescribeSubCommand, tcDescParams)
  mkSubCommand(jcTableConf, UpdateDommand, tcUpdateParams)

  def execute() = {
    //TODO enable
    //val tableTools = new TableTools(null, null)
    jcTableConf.getParsedCommand match {
      case ListSubCommand     =>
        println("tableconf list")
        //tableTools.listConfig()
      case DescribeSubCommand =>
        println("tableconf describe")
        //tableTools.describeConfig()

      case UpdateDommand   =>
        println("tableconf update")
        //tableTools.updateConfig()
    }
  }

  @Parameters
  class TableConfParams {}

  @Parameters
  class ListParams  {
    @Parameter(names = Array("--suffix", "-s"), description = "Table suffix to operate on (attr_idx, st_idx, or records)", required = true)
    var suffix: String = null

    @ParametersDelegate
    var featureParams = new FeatureParams()

  }

  class DescribeParams extends ListParams {
    @Parameter(names = Array("--param"), description = "Accumulo table configuration param name (e.g. table.bloom.enabled)", required = true)
    var param: String = null
  }

  class UpdateParams extends DescribeParams {
    @Parameter(names = Array("--new-value", "-n"), description = "New value of the property)", required = true)
    var newValue: String = null
  }
  
}

object TableConfCommand {
  val Command = "tableconf"
  val ListSubCommand = "list"
  val DescribeSubCommand = "describe"
  val UpdateDommand = "update"
}

