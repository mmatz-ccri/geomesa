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

package org.locationtech.geomesa.jobs.index

import com.twitter.scalding._
import org.apache.accumulo.core.data.{Range => AcRange}
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.jobs.scalding.ConnectionParams._
import org.locationtech.geomesa.jobs.scalding._

import scala.collection.JavaConverters._

class FeatureCopyJob(args: Args) extends GeoMesaBaseJob(args) {

  val feature = args(FEATURE_IN)
  val dsInParams = toDataStoreInParams(args)
  val dsOutParams = toDataStoreOutParams(args)

  val input = GeoMesaInputOptions(dsInParams, feature)
  val output = GeoMesaOutputOptions(dsOutParams)

  // validation
  {
    val dsIn = DataStoreFinder.getDataStore(dsInParams.asJava).asInstanceOf[AccumuloDataStore]
    assert(dsIn != null, "The specified input data store could not be created - check your job parameters")
    val dsOut = DataStoreFinder.getDataStore(dsOutParams.asJava).asInstanceOf[AccumuloDataStore]
    assert(dsOut != null, "The specified output data store could not be created - check your job parameters")
    val sft = dsIn.getSchema(feature)
    assert(sft != null, s"The feature '$feature' does not exist in the input data store")
  }

  // scalding job
  GeoMesaSource(input).write(GeoMesaSource(output))
}


