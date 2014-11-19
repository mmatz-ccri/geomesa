package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{Parameter, ParametersDelegate, JCommander}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.FeaturesTool
import org.locationtech.geomesa.tools.commands.DeleteCommand._

class DeleteCommand(parent: JCommander) extends Logging {

  val params = new DeleteParams
  parent.addCommand(Command, params)

  def execute() = {
    val feature = params.featureName
    val catalog = params.catalog

    val confirmed =
      if (params.forceDelete) {
        true
      } else {
        print(s"Delete '$feature' from catalog table '$catalog'? (yes/no): ")
        System.console().readLine().toLowerCase().trim == "yes"
      }

    if (confirmed) {
      println(s"Deleting '$catalog:$feature'. This will take longer " +
        "than other commands to complete. Just a few moments...")
      try {
        val ds = new DataStoreStuff(params).ds
        ds.removeSchema(feature)
        if (!ds.getNames.contains(feature)) {
          println(s"Feature '$catalog:$feature' successfully deleted.")
        } else {
         println(s"There was an error deleting feature '$catalog:$feature'" +
            "Please check that all arguments are correct in the previous command.")
        }
      } catch {
        case e: Exception =>
          logger.error("Error deleting feature '$catalog:$feature': "+e.getMessage, e)
      }
    } else {
      println("Deleted feature '$catalog:$feature' cancelled")
    }

  }

}

object DeleteCommand {
  val Command = "delete"

  class DeleteParams extends FeatureParams {
    @Parameter(names = Array("--force"), description = "Force deletion of feature without prompt", required = false)
    var forceDelete: Boolean = false
  }
}
