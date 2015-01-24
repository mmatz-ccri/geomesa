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

import java.util.regex.Pattern

import com.beust.jcommander.{JCommander, Parameters, ParametersDelegate}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.tools.commands.DeleteFeatureCommand.{promptConfirm, _}

class DeleteFeatureCommand(parent: JCommander) extends CatalogCommand(parent) with Logging {
  override val command = "delete-feature"
  override val params = new DeleteFeatureParams

  override def execute() = {
    val catalog = params.catalog
    val features = determineFeatures(params.pattern, catalog, params.featureName, ds)
    validate(features, ds)

    if (params.force || promptConfirm(features, catalog)) {
      features.foreach(remove(_, catalog, ds))
    } else {
      logger.info(s"Cancelled deletion")
    }
  }

  def remove(feature: String, catalog: String, ds: AccumuloDataStore) =
    try {
      ds.removeSchema(feature)
      if (!ds.getNames.contains(feature)) {
        println(s"Deleted $catalog:$feature")
      } else {
        logger.error(s"There was an error deleting feature '$catalog:$feature'")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error deleting feature '$catalog:$feature': " + e.getMessage, e)
    }

}

object DeleteFeatureCommand {

  def promptConfirm(features: List[String], catalog: String) =
    PromptConfirm.confirm(s"Delete ${features.mkString(",")} from catalog table $catalog? (yes/no): ")

  def determineFeatures(pattern: Pattern, catalog: String, featureName: String, ds: AccumuloDataStore) = {
    val patternFeatures = Option(pattern).map { p =>
      listFeatures(ds, catalog, Option(p))
    }.getOrElse(List.empty[String])
    Option(featureName).toList ++ patternFeatures
  }

  def validate(features: List[String], ds: AccumuloDataStore) = {
    if (features.isEmpty) {
      throw new IllegalArgumentException("No features found from pattern or feature name")
    }

    val validFeatures = ds.getTypeNames
    features.foreach { f =>
      if (!validFeatures.contains(f)) {
        throw new IllegalArgumentException(s"Feature $f does not exist in datastore")
      }
    }
  }

  def listFeatures(ds: AccumuloDataStore, catalog: String, pattern: Option[Pattern]) = {
    val filter: String => Boolean =
      pattern.map(p => (s: String) => p.matcher(s).matches()).getOrElse( (s: String) => true)
    ds.getTypeNames.filter(filter).toList
  }
  
  @Parameters(commandDescription = "Delete a feature's data and definition from a GeoMesa catalog")
  class DeleteFeatureParams extends OptionalFeatureParams {
    @ParametersDelegate
    val forceParams = new ForceParams
    def force = forceParams.force

    @ParametersDelegate
    val patternParams = new PatternParams
    def pattern = patternParams.pattern
  }

}
