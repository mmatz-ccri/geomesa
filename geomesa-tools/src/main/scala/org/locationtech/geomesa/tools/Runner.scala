package org.locationtech.geomesa.tools

import com.beust.jcommander.JCommander
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.commands._

object Runner extends Logging {

  object MainArgs {}

  def main(args: Array[String]): Unit = {
    val jc        = new JCommander(MainArgs, args.toArray: _*)
    val tableConf = new TableConfCommand(jc)
    val listCom   = new ListCommand(jc)
    val export    = new ExportCommand(jc)
    val delete    = new DeleteCommand(jc)
    val describe  = new DescribeCommand(jc)
    val ingest    = new IngestCommand(jc)
    val create    = new CreateCommand(jc)
    val explain   = new ExplainCommand(jc)

    val command: Command =
      jc.getParsedCommand match {
        case TableConfCommand.Command => tableConf
        case ListCommand.Command      => listCom
        case ExportCommand.Command    => export
        case DeleteCommand.Command    => delete
        case DescribeCommand.Command  => describe
        case IngestCommand.Command    => ingest
        case CreateCommand.Command    => create
        case ExplainCommand.Command   => explain
      }

    try {
      command.execute()
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  def mkSubCommand(parent: JCommander, name: String, obj: Object): JCommander = {
    parent.addCommand(name, obj)
    parent.getCommands().get(name)
  }
}

