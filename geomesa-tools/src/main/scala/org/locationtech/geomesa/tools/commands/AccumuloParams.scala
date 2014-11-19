package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{ParametersDelegate, Parameter}
import org.locationtech.geomesa.tools.AccumuloProperties

class AccumuloParams {
  @Parameter(names = Array("--user", "-u"), description = "Accumulo user name", required = true)
  var user: String = null

  @Parameter(names = Array("--password", "-p"), description = "Accumulo password", password = true, required = true, echoInput = true)
  var password: String = null

  @Parameter(names = Array("--instance", "-i"), description = "Accumulo instance name")
  var instance: String = null

  @Parameter(names = Array("--zookeepers", "-z"), description = "Zookeepers (host:port, comma separated)")
  var zookeepers: String = null

  @Parameter(names = Array("--auths"), description = "Accumulo authorizations")
  var auths: String = null

  @Parameter(names = Array("--visibilities"), description = "Accumulo scan visibilities")
  var visibilities: String = null
}

class GeoMesaParams extends AccumuloParams {
  @Parameter(names = Array("--catalog", "-c"), description = "Catalog table name for GeoMesa", required = true)
  var catalog: String = null
}

class FeatureParams extends GeoMesaParams{
  @Parameter(names = Array("--feature-name", "-f"), description = "Simple Feature Type name on which to operate", required = true)
  var featureName: String = null
}

class CqlParams extends FeatureParams{
  @Parameter(names = Array("-q", "--query", "--filter"), description = "CQL Filter to use for the query explain", required = true)
  var cqlFilter: String = null
}
