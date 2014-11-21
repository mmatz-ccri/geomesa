package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.commands.ListCommand._

// TODO consider the former -quiet option...?
class ListCommand(parent: JCommander) extends Command {

  val params = new GeoMesaParams
  parent.addCommand(Command, params)
  lazy val ds = new DataStoreStuff(params).ds

  override def execute() = {
    val types = ds.getTypeNames
    val numTypes = types.size

    val msg =
      numTypes match {
        case 0 => s"0 features exist in the catalog '${params.catalog}'. This catalog table may not yet exist."
        case 1 => s"1 feature exists in the catalog '${params.catalog}'. It is: "
        case _ => s"${numTypes} features exist in the catalog '${params.catalog}'. They are: "
      }
    println(msg)
    types.foreach(println)
  }

}

object ListCommand {
  val Command = "list"
}
