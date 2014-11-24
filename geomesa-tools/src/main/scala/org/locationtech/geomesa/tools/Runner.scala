/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

