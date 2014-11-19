package org.locationtech.geomesa.tools.commands

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.tools.AccumuloProperties

import scala.collection.JavaConversions._

import scala.util.Try


class DataStoreStuff(params: GeoMesaParams) extends AccumuloProperties {
  lazy val instance = Option(params.instance).getOrElse(instanceName)
  lazy val zookeepersString = Option(params.zookeepers).getOrElse(zookeepersProp)

  lazy val ds: AccumuloDataStore = Try({
    DataStoreFinder.getDataStore(
      Map(
        "instanceId"   -> instance,
        "zookeepers"   -> zookeepersString,
        "user"         -> params.user,
        "password"     -> params.password,
        "tableName"    -> params.catalog,
        "visibilities" -> Option(params.visibilities).orNull,
        "auths"        -> Option(params.auths).orNull)
      ).asInstanceOf[AccumuloDataStore]
    }).getOrElse{
      logger.error("Cannot connect to Accumulo. Please check your configuration and try again.")
      sys.exit()
    }
}
