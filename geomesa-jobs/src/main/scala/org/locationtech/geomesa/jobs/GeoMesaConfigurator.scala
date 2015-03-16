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

package org.locationtech.geomesa.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import scala.collection.JavaConversions._

object GeoMesaConfigurator {

  private val prefix = "org.locationtech.geomesa"
  private val dsParams = s"$prefix.params."
  private val dsRegex = dsParams.replaceAll("\\.", "\\.") + ".+"
  private val dsSubstring = dsParams.length
  private val filterKey = s"$prefix.filter"
  private val sftKey = s"$prefix.sft"

  def setDataStoreParameters(conf: Configuration, params: Map[String, String]): Unit =
    params.foreach { case (key, value) => conf.set(s"$dsParams$key", value) }
  def getDataStoreParameters(job: Job): Map[String, String] =
    getDataStoreParameters(job.getConfiguration)
  def getDataStoreParameters(conf: Configuration): Map[String, String] =
    conf.getValByRegex(dsRegex).map { case (key, value) => (key.substring(dsSubstring), value) }.toMap

  def setFeatureType(conf: Configuration, featureType: String): Unit =
    conf.set(sftKey, featureType)
  def getFeatureType(job: Job): String = getFeatureType(job.getConfiguration)
  def getFeatureType(conf: Configuration): String = conf.get(sftKey)

  def setFilter(conf: Configuration, filter: String): Unit = conf.set(filterKey, filter)
  def getFilter(job: Job): Option[String] = getFilter(job.getConfiguration)
  def getFilter(conf: Configuration): Option[String] = Option(conf.get(filterKey))
}
