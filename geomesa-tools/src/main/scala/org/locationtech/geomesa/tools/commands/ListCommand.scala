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
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.DataStoreHelper
import org.locationtech.geomesa.tools.commands.ListCommand._

// TODO consider the former -quiet option...?
class ListCommand(parent: JCommander) extends Command {

  val params = new GeoMesaParams
  parent.addCommand(Command, params)
  lazy val ds = new DataStoreHelper(params).ds

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
