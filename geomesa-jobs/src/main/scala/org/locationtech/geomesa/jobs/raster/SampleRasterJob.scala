package org.locationtech.geomesa.jobs.raster

import com.twitter.scalding._
import org.apache.accumulo.core.data.{Mutation, Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
import org.locationtech.geomesa.feature.SimpleFeatureDecoder
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.scalding._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.collection.mutable

trait RasterJobResources {
  def ds: AccumuloDataStore
  def sft: SimpleFeatureType
  def visibilities: String
  def decoder: SimpleFeatureDecoder
  def attributeDescriptors: mutable.Buffer[(Int, AttributeDescriptor)]

  // required by scalding
  def release(): Unit = {}
}

object RasterJobResources {
  import scala.collection.JavaConversions._
  def apply(params:  Map[String, String], feature: String, attributes: List[String]) = new RasterJobResources {
    val ds: AccumuloDataStore = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    val sft: SimpleFeatureType = ds.getSchema(feature)
    val visibilities: String = ds.writeVisibilities
    val decoder: SimpleFeatureDecoder = SimpleFeatureDecoder(sft, ds.getFeatureEncoding(sft))
    // the attributes we want to index
    override val attributeDescriptors =
      sft.getAttributeDescriptors
        .zipWithIndex
        .filter { case (ad, idx) => attributes.contains(ad.getLocalName) }
        .map { case (ad, idx) => (idx, ad) }
  }
}

class SampleRasterJob(args: Args) extends Job(args) {

//  lazy val zookeepers       = args(ConnectionParams.ZOOKEEPERS)
//  lazy val instance         = args(ConnectionParams.ACCUMULO_INSTANCE)
//  lazy val user             = args(ConnectionParams.ACCUMULO_USER)
//  lazy val password         = args(ConnectionParams.ACCUMULO_PASSWORD)
  //lazy val tablename        = args(ConnectionParams.CATALOG_TABLE)

  //val tablename = "AANNEX_SRI_ALL_VIS_RASTERS"

  val inputTable = "Aannex_sri_raster_1"
  val outputTable = "jnh_mr1"
  //lazy val input   = AccumuloInputOptions(inputTable, authorizations = new Authorizations("S", "USA"))
  lazy val input   = AccumuloInputOptions(inputTable)
  lazy val output  = AccumuloOutputOptions("jnh_mr2", createTable = true)
  lazy val options = AccumuloSourceOptions("dcloud", "dzoo1", "root", "secret", input, output)

  //println(s"Args decode to $zookeepers $instance $user $password $inputTable")

//  AccumuloSource(options)
//    .map(('key, 'value) -> ('line, 'number)) {
//    (kv: (Key, Value)) => ("foo", 2)
//  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output"))

//  AccumuloSource(options)
//    .mapTo('line) {
//      (kv: (Key, Value)) => s"rowId ${kv._1.getRow}"
//  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output"))

//  AccumuloSource(options)
//    .mapTo('mutation) {
//    (kv: (Key, Value)) => SampleRasterJob.kvToMutation(kv._1, kv._2)
//  }.write(AccumuloSource(options))

  AccumuloSource(options)
    .map(('key, 'value) -> 'mutation) {
      (kv: (Key, Value)) => {
        SampleRasterJob.kvToMutation(kv._1, kv._2)
      }
  }.write(AccumuloSource(options))
}


object SampleRasterJob {

  def kvToMutation(k: Key, v: Value): Mutation = {
    val m = new Mutation(k.getRow)

    m.put(k.getColumnFamily, k.getColumnQualifier, k.getColumnVisibilityParsed, v)
    m
  }

  val conf = new Configuration
  conf.set("accumulo.monitor.address", "damaster"
  )
  conf.set("mapreduce.framework.name", "yarn")
  conf.set("yarn.resourcemanager.address", "dresman:8040")
  conf.set("fs.defaultFS", "hdfs://dhead:54310")
  conf.set("yarn.resourcemanager.scheduler.address", "dresman:8030")

  def runJob() = {
    JobUtils.setLibJars(conf)

    val args = new collection.mutable.ListBuffer[String]()

    args.append("--" + ConnectionParams.ZOOKEEPERS, "dzoo1")
    args.append("--" + ConnectionParams.ACCUMULO_INSTANCE, "dcloud")
    args.append("--" + ConnectionParams.ACCUMULO_USER, "root")
    args.append("--" + ConnectionParams.ACCUMULO_PASSWORD, "secret")
    //args.append("--" + ConnectionParams.CATALOG_TABLE, "AANNEX_SRI_ALL_VIS_RASTERS")

    val args2 = Args(args)

    val hdfsMode = Hdfs(strict = true, conf)
    val arguments = Mode.putMode(hdfsMode, args2)

    val job = new SampleRasterJob(arguments)
    val flow = job.buildFlow
    flow.complete() // this blocks until the job is done
  }

}