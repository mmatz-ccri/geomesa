package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander

object Runner {

  object MainArgs {}

  //http://stackoverflow.com/questions/10160086/java-cli-parser?answertab=active#tab-top
  def main(args: Array[String]): Unit = {

    // Java class in this project to bridge varargs in Java
//    val jc = JCommanderBridge.create(MainArgs)
//    jc.setProgramName("java -jar <jarfile>")

    val jc = new JCommander(MainArgs, args.toArray: _*)
    val tableConf = new TableConfCommand(jc)
    val listCom = new ListCommand(jc)
    val export  = new ExportCommand(jc)



    //jc.parse("tableconf", "list", "-s", "foo", "-u", "user", "-f", "feat", "-c", "la", "-p")
    //jc.parse("list", "-u", "user", "-p", "-c", "catalog")
//    jc.parse("export",
//      "-u", "user",
//      "-p",
//      "-c", "catalog",
//      "-f", "featname",
//      "-s",
//      "--format", "csv",
//      "--idAttribute", "idAttr",
//      "--latAttribute", "latAttr",
//      "--lonAttribute", "lonAttr",
//      "--dateAttribute", "dateAttr",
//      "--maxFeatures", "236236",
//      "--query" ,"INCLUDE")

    jc.getParsedCommand match {
      case TableConfCommand.Command => tableConf.execute()
      case ListCommand.Command      => listCom.execute()
      case ExportCommand.Command    => export.execute()
    }

  }

  def mkSubCommand(parent: JCommander, name: String, obj: Object): JCommander = {
    parent.addCommand(name, obj);
    parent.getCommands().get(name);
  }
}

