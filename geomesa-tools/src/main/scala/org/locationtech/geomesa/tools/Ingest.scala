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

import java.io.File
import java.net.URLEncoder

import com.twitter.scalding.{Args, Hdfs, Local, Mode}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.Connector
import org.apache.hadoop.conf.Configuration
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.tools.Utils.IngestParams

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.{Failure, Success, Try}

class Ingest() extends Logging with AccumuloProperties {

  def createDataStoreSchema(config: IngestArguments, password: String) = {
    if (config.featureName.isEmpty) {
      logger.error("No feature name specified for CSV/TSV Ingest." +
        " Please check that all arguments are correct in the previous command. ")
      sys.exit()
    }

    val dsConfig = getAccumuloDataStoreConf(config, password)

    Try(DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]) match {
      case Success(ds)  =>
        FeatureCreator.createFeature(ds, config.spec, config.featureName, config.dtField,
          config.sharedTable, config.catalog, config.maxShards)
      case Failure(ex) =>
        logger.error("Error, could not find data store with provided arguments." +
          " Please check that all arguments are correct in the previous command")
        logger.error(ex.getMessage)
        sys.exit()
    }

  }

  def getAccumuloDataStoreConf(config: IngestArguments, password: String) = Map (
      "instanceId"        -> config.instanceName.getOrElse(instanceName),
      "zookeepers"        -> config.zookeepers.getOrElse(zookeepersProp),
      "user"              -> config.username,
      "password"          -> password,
      "tableName"         -> config.catalog,
      "auths"             -> config.auths,
      "visibilities"      -> config.visibilities,
      "maxShard"          -> config.maxShards,
      "indexSchemaFormat" -> config.indexSchemaFmt
    ).collect{ case (key, Some(value)) => (key, value); case (key, value: String) => (key, value) }

  def defineIngestJob(config: IngestArguments, password: String) = {
    Ingest.getFileExtension(config.file).toUpperCase match {
      case "CSV" | "TSV" =>
        Ingest.getFileSystemMethod(config.file).toLowerCase match {
          case "local" =>
            logger.info("Local Ingest has started, please wait.")
            runIngestJob(config, "--local", password)
          case "hdfs" =>
            logger.info("Map-reduced Ingest has started, please wait.")
            runIngestJob(config, "--hdfs", password)
          case _ =>
            logger.error("Error, no such ingest method for CSV or TSV found, no data ingested")
        }
      case "SHP" =>
        val dsConfig = getAccumuloDataStoreConf(config, password)
        ShpIngest.doIngest(config, dsConfig)
      case _ =>
        logger.error(s"Error: file format not supported or not found in provided file path." +
          s" Supported formats include: CSV, TSV, and SHP. No data ingested.")

    }
  }

  def ingestLibJars = {
    val defaultLibJarsFile = "org/locationtech/geomesa/tools/ingest-libjars.list"
    val url = Try(getClass.getClassLoader.getResource(defaultLibJarsFile))
    val source = url.map(Source.fromURL)
    val lines = source.map(_.getLines().toList)
    source.foreach(_.close())
    lines.get
  }

  def ingestJarSearchPath: Iterator[() => Seq[File]] =
    Iterator(() => JobUtils.getJarsFromEnvironment("GEOMESA_HOME"),
      () => JobUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
      () => JobUtils.getJarsFromClasspath(classOf[SVIngest]),
      () => JobUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
      () => JobUtils.getJarsFromClasspath(classOf[Connector]))

  def runIngestJob(config: IngestArguments, fileSystem: String, password: String): Unit = {
    // create data store schema outside of map-reduce
    createDataStoreSchema(config, password)

    val conf = new Configuration()
    JobUtils.setLibJars(conf, libJars = ingestLibJars, searchPath = ingestJarSearchPath)

    val args = new collection.mutable.ListBuffer[String]()
    args.append(classOf[SVIngest].getCanonicalName)
    args.append(fileSystem)
    args.append("--" + IngestParams.FILE_PATH, config.file)
    args.append("--" + IngestParams.SFT_SPEC, URLEncoder.encode(config.spec, "UTF-8"))
    args.append("--" + IngestParams.CATALOG_TABLE, config.catalog)
    args.append("--" + IngestParams.ZOOKEEPERS, config.zookeepers.getOrElse(zookeepersProp))
    args.append("--" + IngestParams.ACCUMULO_INSTANCE, config.instanceName.getOrElse(instanceName))
    args.append("--" + IngestParams.ACCUMULO_USER, config.username)
    args.append("--" + IngestParams.ACCUMULO_PASSWORD, password)
    args.append("--" + IngestParams.DO_HASH, config.doHash.toString)
    args.append("--" + IngestParams.FORMAT, Ingest.getFileExtension(config.file))
    args.append("--" + IngestParams.FEATURE_NAME, config.featureName)

    // optional parameters
    if ( config.cols.isDefined )            args.append("--" + IngestParams.COLS, config.cols.get)
    if ( config.dtFormat.isDefined )        args.append("--" + IngestParams.DT_FORMAT, config.dtFormat.get)
    if ( config.idFields.isDefined )        args.append("--" + IngestParams.ID_FIELDS, config.idFields.get)
    if ( config.dtField.isDefined )         args.append("--" + IngestParams.DT_FIELD, config.dtField.get)
    if ( config.lonAttribute.isDefined )    args.append("--" + IngestParams.LON_ATTRIBUTE, config.lonAttribute.get)
    if ( config.latAttribute.isDefined )    args.append("--" + IngestParams.LAT_ATTRIBUTE, config.latAttribute.get)
    if ( config.auths.isDefined )           args.append("--" + IngestParams.AUTHORIZATIONS, config.auths.get)
    if ( config.visibilities.isDefined )    args.append("--" + IngestParams.VISIBILITIES, config.visibilities.get)
    if ( config.indexSchemaFmt.isDefined )  args.append("--" + IngestParams.INDEX_SCHEMA_FMT, config.indexSchemaFmt.get)
    if ( config.maxShards.isDefined )       args.append("--" + IngestParams.SHARDS, config.maxShards.get.toString)
    // If we are running a test ingest, then set to true, default is false
    args.append("--" + IngestParams.IS_TEST_INGEST, config.dryRun.toString)
    if ( config.dtField.isEmpty ) {
      // assume user has no date field to use and that there is no column of data signifying it.
      logger.warn("Warning: no date-time field specified. Assuming that data contains no date column. \n" +
        s"GeoMesa is defaulting to the system time for ingested features.")
    }
    val scaldingArgs = Args(args)
    // continue with ingest
    val hdfsMode = if (fileSystem == "--hdfs") Hdfs(strict = true, conf) else Local(strictSources = true)
    val arguments = Mode.putMode(hdfsMode, scaldingArgs)
    val job = new SVIngest(arguments)
    val flow = job.buildFlow
    //block until job is completed.
    flow.complete()
    job.printStatInfo
  }
}

object Ingest extends App with Logging with GetPassword {

  def getFileExtension(file: String) = file.toLowerCase match {
    case csv if file.endsWith("csv") => "CSV"
    case tsv if file.endsWith("tsv") => "TSV"
    case shp if file.endsWith("shp") => "SHP"
    case _                           => "NOTSUPPORTED"
  }

  def getFileSystemMethod(path: String): String = path.toLowerCase.startsWith("hdfs") match {
    case true => "hdfs"
    case _    => "local"
  }

}
