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
