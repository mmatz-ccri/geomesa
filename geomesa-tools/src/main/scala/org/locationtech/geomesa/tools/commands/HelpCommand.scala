package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander

class HelpCommand(jc: JCommander) extends Command {

  override def execute(): Unit = {
    jc.usage()
  }
}
