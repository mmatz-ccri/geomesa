package org.locationtech.geomesa.tools.commands

import java.util

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.tools.commands.HelpCommand.HelpParameters
import org.locationtech.geomesa.tools.Runner.commandUsage

import scala.collection.JavaConversions._

class HelpCommand(parent: JCommander) extends Command {

  val params = new HelpParameters
  parent.addCommand(HelpCommand.Command, params)

  override def execute(): Unit =
    params.commandName.headOption match {
      case Some(command) => parent.usage(command)
      case None          =>
        println(commandUsage(parent) + "\nTo see help for a specific command type: geomesa help <command-name>\n")
    }

}

object HelpCommand {
  val Command = "help"

  @Parameters(commandDescription = "Show help")
  class HelpParameters {
    @Parameter(description = "commandName", required = false)
    val commandName: util.List[String] = new util.ArrayList[String]()
  }

}
