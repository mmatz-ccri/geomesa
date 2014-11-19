/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.tools

import java.io.{File, FileOutputStream}

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.joda.time.DateTime
import org.locationtech.geomesa.core.data.AccumuloFeatureStore
import org.locationtech.geomesa.tools.commands.DataStoreStuff
import org.locationtech.geomesa.tools.commands.ExportCommand.ExportParams
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.Try

class Export(params: ExportParams) extends DataStoreStuff(params) with Logging {

  def exportFeatures() = {
    val out = createUniqueFileName()
    params.format.toLowerCase match {
      case "csv" | "tsv"       => exportDelimitedText(out)
      case "shp"               => exportShapeFile(out)
      case "geojson" | "json"  => exportGeoJson(out)
      case "gml"               => exportGML(out)
      case _ =>
        logger.error("Unsupported export format. Supported formats are shp, geojson, csv, and gml.")
    }
    Thread.sleep(1000)
  }

  def createUniqueFileName(): File = {
    var outputPath: File = null
    do {
      if (outputPath != null) { Thread.sleep(1) }
      outputPath = new File(s"${System.getProperty("user.dir")}/${params.catalog}_${params.featureName}_${DateTime.now()}.${params.format}")
    } while (outputPath.exists)
    outputPath
  }

  def exportDelimitedText(outputFile: File) = {
    val sftCollection = getFeatureCollection()
    val loadAttributes = new LoadAttributes(params.featureName,
      params.catalog,
      params.attributes,
      params.idAttribute,
      Option(params.latAttribute),
      Option(params.lonAttribute),
      Option(params.dateAttribute),
      params.cqlFilter,
      params.format,
      params.stdOut,
      outputFile)
    val de = new SVExport(loadAttributes, Map(
      "instanceId"   -> instance,
      "zookeepers"   -> zookeepersString,
      "user"         -> params.user,
      "password"     -> params.password,
      "tableName"    -> params.catalog,
      "visibilities" -> params.visibilities,
      "auths"        -> params.auths))
    de.writeFeatures(sftCollection.features())
  }

  def exportShapeFile(outputFile: File) = {
    // When exporting to Shapefile, we must rename the Geometry Attribute Descriptor to "the_geom", per
    // the requirements of Geotools' ShapefileDataStore and ShapefileFeatureWriter. The easiest way to do this
    // is transform the attribute when retrieving the SimpleFeatureCollection.
    val attrDescriptors = Option(params.attributes).getOrElse(
      ds.getSchema(params.featureName).getAttributeDescriptors.map(_.getLocalName).mkString(","))
    val geomDescriptor = ds.getSchema(params.featureName).getGeometryDescriptor.getLocalName
    val renamedGeomAttrs = if (attrDescriptors.contains(geomDescriptor)) {
      attrDescriptors.replace(geomDescriptor, s"the_geom=$geomDescriptor")
    } else {
      attrDescriptors.concat(s",the_geom=$geomDescriptor")
    }
    val shpCollection = getFeatureCollection(Some(renamedGeomAttrs))
    val shapeFileExporter = new ShapefileExport
    shapeFileExporter.write(outputFile, params.featureName, shpCollection, shpCollection.getSchema)
    logger.info(s"Successfully wrote features to '${outputFile.toString}'")
  }

  def exportGeoJson(outputFile: File) = {
    val sftCollection = getFeatureCollection()
    val os = if (params.stdOut) { System.out } else { new FileOutputStream(outputFile) }
    val geojsonExporter = new GeoJsonExport
    geojsonExporter.write(sftCollection, os)
    if (!params.stdOut) { logger.info(s"Successfully wrote features to '$outputFile'") }
  }

  def exportGML(outputFile: File) = {
    val sftCollection = getFeatureCollection()
    val os = if (params.stdOut) { System.out } else { new FileOutputStream(outputFile) }
    val gmlExporter = new GmlExport
    gmlExporter.write(sftCollection, os)
    if (!params.stdOut) { logger.info(s"Successfully wrote features to '$outputFile'") }
  }

  def getFeatureCollection(overrideAttributes: Option[String] = None): SimpleFeatureCollection = {
    val filter = Option(params.cqlFilter).map(CQL.toFilter).getOrElse(Filter.INCLUDE)
    val q = new Query(params.featureName, filter)

    q.setMaxFeatures(Option(params.maxFeatures).getOrElse(Query.DEFAULT_MAX.asInstanceOf[Integer]))
    val attributesO = if (overrideAttributes.isDefined) overrideAttributes
                      else if (Option(params.attributes).isDefined) Some(params.attributes)
                      else None
    //Split attributes by "," meanwhile allowing to escape it by "\,".
    attributesO.foreach { attributes =>
      q.setPropertyNames(attributes.split("""(?<!\\),""").map(_.trim.replace("\\,", ",")))
    }

    // get the feature store used to query the GeoMesa data
    val fs = ds.getFeatureSource(params.featureName).asInstanceOf[AccumuloFeatureStore]

    // and execute the query
    Try(fs.getFeatures(q)).getOrElse{
      logger.error("Error: Could not create a SimpleFeatureCollection to export. Please ensure " +
        "that all arguments are correct in the previous command.")
      sys.exit()
    }
  }
}