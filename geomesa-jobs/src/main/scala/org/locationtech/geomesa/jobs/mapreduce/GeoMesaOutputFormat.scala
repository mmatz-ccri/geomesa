/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.jobs.mapreduce

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object GeoMesaOutputFormat {

  def configure(job: org.apache.hadoop.mapreduce.Job, dsParams: Map[String, String]): Unit = {
    val conf = job.getConfiguration

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

    assert(ds != null, "Invalid data store parameters")

    // set up the underlying accumulo input format
    val user = AccumuloDataStoreFactory.params.userParam.lookUp(dsParams).asInstanceOf[String]
    val password = AccumuloDataStoreFactory.params.passwordParam.lookUp(dsParams).asInstanceOf[String]
    AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(password.getBytes()))

    val instance = AccumuloDataStoreFactory.params.instanceIdParam.lookUp(dsParams).asInstanceOf[String]
    val zookeepers = AccumuloDataStoreFactory.params.zookeepersParam.lookUp(dsParams).asInstanceOf[String]
    AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers)

    AccumuloOutputFormat.setCreateTables(job, false)

    GeoMesaConfigurator.setDataStoreParameters(job.getConfiguration, dsParams)
//    AccumuloOutputFormat.setDefaultTableName(conf, options.output.table)

//    val batchWriterConfig = new BatchWriterConfig()
//    options.output.threads.foreach(t => batchWriterConfig.setMaxWriteThreads(t))
//    options.output.memory.foreach(m => batchWriterConfig.setMaxMemory(m))
//    AccumuloOutputFormat.setBatchWriterOptions(conf, batchWriterConfig)

  }
}

class GeoMesaOutputFormat extends org.apache.hadoop.mapreduce.OutputFormat[String, SimpleFeature] {

  val delegate: org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat =
    new org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat

  override def getRecordWriter(context: TaskAttemptContext) = {
    val params = GeoMesaConfigurator.getDataStoreParameters(context.getConfiguration)
    new GeoMesaRecordWriter(params, delegate.getRecordWriter(context))
  }

  override def checkOutputSpecs(context: JobContext) = {
    delegate.checkOutputSpecs(context)
  }

  override def getOutputCommitter(context: TaskAttemptContext) = {
    delegate.getOutputCommitter(context)
  }
}

class GeoMesaRecordWriter(params: Map[String, String],
                          delegate: org.apache.hadoop.mapreduce.RecordWriter[Text, Mutation])
    extends org.apache.hadoop.mapreduce.RecordWriter[String, SimpleFeature] {

  val ds = DataStoreFinder.getDataStore(params)

  override def write(key: String, value: SimpleFeature) = {

  }

  override def close(context: TaskAttemptContext) = {
    delegate.close(context)
  }
}