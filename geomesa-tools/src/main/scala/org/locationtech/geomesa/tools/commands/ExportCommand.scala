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

import java.io.File

import com.beust.jcommander.{JCommander, Parameter}
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.joda.time.DateTime
import org.locationtech.geomesa.core.data.AccumuloFeatureStore
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.commands.ExportCommand.{Command, ExportParams}
import org.opengis.filter.Filter

import scala.util.Try

class ExportCommand(parent: JCommander) extends Command with Logging {

  val params = new ExportParams
  parent.addCommand(Command, params)

  override def execute() = {

    val fmt = params.format.toLowerCase()
    val exporter: FeatureExporter = fmt match {
      case CSV | TSV       =>
        new DelimitedExport(fmt,
          Option(params.attributes),
          Option(params.idAttribute),
          Option(params.latAttribute),
          Option(params.lonAttribute),
          Option(params.dateAttribute))
      case SHP             => new ShapefileExport()
      case GeoJson | JSON  => new GeoJsonExport()
      case GML             => new GmlExport()
      case _ =>
        throw new IllegalArgumentException("Unsupported export format. Supported formats are shp, geojson, csv, and gml.")
    }

    val outFile = createUniqueFileName()
    val features = getFeatureCollection()
    exporter.write(features, outFile)
  }

  def getFeatureCollection(overrideAttributes: Option[String] = None): SimpleFeatureCollection = {
    val filter = Option(params.cqlFilter).map(CQL.toFilter).getOrElse(Filter.INCLUDE)
    val q = new Query(params.featureName, filter)
    q.setMaxFeatures(Option(params.maxFeatures).getOrElse(Query.DEFAULT_MAX.asInstanceOf[Integer]).toInt)

    val attributesO = if (overrideAttributes.isDefined) overrideAttributes
    else if (Option(params.attributes).isDefined) Some(params.attributes)
    else None
    //Split attributes by "," meanwhile allowing to escape it by "\,".
    attributesO.foreach { attributes =>
      q.setPropertyNames(attributes.split("""(?<!\\),""").map(_.trim.replace("\\,", ",")))
    }

    // get the feature store used to query the GeoMesa data
    val fs = new DataStoreHelper(params).ds.getFeatureSource(params.featureName).asInstanceOf[AccumuloFeatureStore]

    // and execute the query
    Try(fs.getFeatures(q)).getOrElse{
      throw new Exception("Error: Could not create a SimpleFeatureCollection to export. Please ensure " +
        "that all arguments are correct in the previous command.")
    }
  }

  // TODO examine this method
  def createUniqueFileName(): File = {
    var outputPath: File = null
    do {
      if (outputPath != null) { Thread.sleep(1) }
      outputPath = new File(s"${System.getProperty("user.dir")}/${params.catalog}_${params.featureName}_${DateTime.now()}.${params.format}")
    } while (outputPath.exists)
    outputPath
  }

}

object ExportCommand {
  val Command = "export"

  class ExportParams extends CqlParams {
    @Parameter(names = Array("--format"), description = "Format to export (csv|tsv|gml|json|shp)", required = true)
    var format: String = null

    @Parameter(names = Array("--maxFeatures", "-m"), description = "Maximum number of features to return. default: Long.MaxValue")
    var maxFeatures: Integer = Int.MaxValue

    @Parameter(names = Array("--stdout", "-s"), description = "flag to force export to std out")
    var stdOut: Boolean = false

    @Parameter(names = Array("--attributes", "-a"), description = "Attributes from feature to export " +
      "(comma-separated)...Comma-separated expressions with each in the format " +
      "attribute[=filter_function_expression]|derived-attribute=filter_function_expression. " +
      "filter_function_expression is an expression of filter function applied to attributes, literals " +
      "and filter functions, i.e. can be nested")
    var attributes: String = null

    @Parameter(names = Array("--idAttribute"), description = "name of the id attribute to export")
    var idAttribute: String = null

    @Parameter(names = Array("--latAttribute"), description = "name of the latitude attribute to export")
    var latAttribute: String = null

    @Parameter(names = Array("--lonAttribute"), description = "name of the longitude attribute to export")
    var lonAttribute: String = null

    @Parameter(names = Array("--dateAttribute"), description = "name of the date attribute to export")
    var dateAttribute: String = null
  }
}
