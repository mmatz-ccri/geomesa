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

import com.beust.jcommander.{JCommander, Parameters, ParametersDelegate}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.commands.DeleteCatalogCommand._

class DeleteCatalogCommand (parent: JCommander) extends CatalogCommand(parent) with Logging {
  override val command = "delete-catalog"
  override val params = new DeleteCatalogParams

  override def execute() = {
    val catalog = params.catalog

    val msg = s"Delete catalog '$catalog'? (re-type catalog name to confirm): "
    if (PromptConfirm.confirm(msg, List(catalog))) {
      ds.delete
    } else {
      logger.info(s"Cancelled deletion")
    }
  }
}

object DeleteCatalogCommand {
  @Parameters(commandDescription = "Delete a GeoMesa catalog completely (and all features in it)")
  class DeleteCatalogParams extends GeoMesaParams {
    @ParametersDelegate
    val forceParams = new ForceParams
    def force = forceParams.force
  }
}
