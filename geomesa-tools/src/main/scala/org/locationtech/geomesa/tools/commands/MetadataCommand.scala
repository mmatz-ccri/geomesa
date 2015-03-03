/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

import com.beust.jcommander.{Parameter, Parameters, JCommander}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.commands.MetadataCommand.MetadataParams

class MetadataCommand(parent: JCommander) extends CommandWithCatalog(parent) with Logging {
  override val command = "metadata"
  override val params = new MetadataParams

  def execute() = {
    logger.info(s"Exporting metadata for feature '${params.featureName}' from catalog table '$catalog'...")

    val keyFilter: ((String, String)) => Boolean =
      Option(params.metadataKey)
        .map(k => (t:(String, String)) => k.split(",").contains(t._1))
        .getOrElse((_) => true)

    val metadata = ds.getMetadataMap(params.featureName).filter(keyFilter)
    metadata.toList.sortBy(_._1).foreach { case (k, v) =>
      println(s"$k: $v")
    }
  }
}

object MetadataCommand {
  @Parameters(commandDescription = "Write out the metadata for a feature or catalog")
  class MetadataParams extends FeatureParams {
    @Parameter(names = Array("-keys", "--metadata-keys"), description = "Retrieve only a specific metadata keys (comma separated)", required = false)
    var metadataKey: String = null
  }
}