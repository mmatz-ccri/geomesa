package org.locationtech.geomesa.tools.commands

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.{params => dsParams}
import org.locationtech.geomesa.tools.AccumuloProperties

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


class DataStoreStuff(params: GeoMesaParams) extends AccumuloProperties {
  lazy val instance = Option(params.instance).getOrElse(instanceName)
  lazy val zookeepersString = Option(params.zookeepers).getOrElse(zookeepersProp)

  lazy val ds: AccumuloDataStore = Try({
    DataStoreFinder.getDataStore(
      Map[String, String](
        dsParams.instanceIdParam.getName   -> instance,
        dsParams.zookeepersParam.getName   -> zookeepersString,
        dsParams.userParam.getName         -> params.user,
        dsParams.passwordParam.getName     -> params.password,
        dsParams.tableNameParam.getName    -> params.catalog,
        dsParams.visibilityParam.getName   -> Option(params.visibilities).orNull,
        dsParams.authsParam.getName        -> Option(params.auths).orNull,
        dsParams.mockParam.getName         -> params.useMock.toString)
      ).asInstanceOf[AccumuloDataStore]
    }) match {
    case Success(value) => value
    case Failure(ex)    => throw new Exception("Cannot connect to Accumulo. Please check your configuration and try again...params are ", ex)
  }
}
