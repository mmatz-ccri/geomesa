package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{Parameter, JCommander}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.FeatureCreator
import org.locationtech.geomesa.tools.commands.CreateCommand.{CreateParams, Command}

class CreateCommand(parent: JCommander) extends Logging {

  val params = new CreateParams()
  parent.addCommand(Command, params)

  def execute() = {
    val ds = new DataStoreStuff(params).ds
    FeatureCreator.createFeature(
      ds,
      params.spec,
      params.featureName,
      Option(params.dtgField),
      Option(params.useSharedTables),
      params.catalog,
      Option(params.numShards)
    )
  }

}

object CreateCommand {
  val Command = "create"

  // TODO common params here...extract into common class
  class CreateParams extends FeatureParams {
    @Parameter(names = Array("--spec", "-s"), description = "SimpleFeatureType specification", required = true)
    var spec: String = null

    @Parameter(names = Array("--dtField", "-d"), description = "DateTime field name to use as the default dtg", required = false)
    var dtgField: String = null

    @Parameter(names = Array("--useSharedTables"), description = "Use shared tables in Accumulo for feature storage (default false)")
    var useSharedTables: Boolean = true

    @Parameter(names = Array("--shards"), description = "Number of shards to use for the storage tables (defaults to number of tservers)")
    var numShards: Integer = null
  }
}