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
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data.AccumuloFeatureReader
import org.locationtech.geomesa.tools.commands.ExplainCommand.Command

class ExplainCommand(parent: JCommander) extends Command with Logging {

  val params = new CqlParams()
  parent.addCommand(Command, params)

  override def execute() =
    try {
      val q = new Query(params.featureName, ECQL.toFilter(params.cqlFilter))
      val ds = new DataStoreStuff(params).ds
      val t = Transaction.AUTO_COMMIT
      val afr = ds.getFeatureReader(q, t).asInstanceOf[AccumuloFeatureReader]
      afr.explainQuery(q)
    } catch {
      case e: Exception =>
        logger.error(s"Error: Could not explain the query (${params.cqlFilter}): ${e.getMessage}", e)
    }

}

object ExplainCommand {
  val Command = "explain"
}
