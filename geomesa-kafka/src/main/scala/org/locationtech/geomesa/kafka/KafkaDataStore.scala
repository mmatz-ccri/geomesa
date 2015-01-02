package org.locationtech.geomesa.kafka

import java.io.Serializable
import java.{util => ju}

import kafka.admin.AdminUtils
import org.I0Itec.zkclient.ZkClient
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry}
import org.geotools.data.{AbstractDataStoreFactory, DataStore}
import org.geotools.feature.NameImpl
import org.opengis.feature.`type`.Name

class KafkaDataStore(broker: String, zookeepers: String, exp: Long)
  extends ContentDataStore {
  import scala.collection.JavaConverters._

  lazy val zkClient = new ZkClient(zookeepers)

  override def createTypeNames() = List.empty[Name].asJava
//    AdminUtils.fetchAllTopicConfigs(zkClient).keys.map(t => new NameImpl(t)).toList.asJava.asInstanceOf[java.util.List[Name]]

  override def createFeatureSource(entry: ContentEntry) = null
/*
    if(createTypeNames().contains(entry.getName))
      new KafkaFeatureSource(entry, zookeepers, RandomStringUtils.randomAlphanumeric(5), exp, null)
    else null
*/
}

class KafkaDataStoreFactory extends AbstractDataStoreFactory {

  val KAFKA_BROKER_PARAM = new Param("broker", classOf[String], "Kafka broker", true)
  val ZOOKEEPERS_PARAM   = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val EXPIRATION_AGE     = new Param("expirationage", classOf[java.lang.Long], "Expiration of features", false, 3600L)

  override def createDataStore(params: ju.Map[String, Serializable]): DataStore = {
    val broker = KAFKA_BROKER_PARAM.lookUp(params).asInstanceOf[String]
    val zk     = ZOOKEEPERS_PARAM.lookUp(params).asInstanceOf[String]
    val exp    =
      if(EXPIRATION_AGE.lookUp(params) != null) EXPIRATION_AGE.lookUp(params).asInstanceOf[java.lang.Long]
      else EXPIRATION_AGE.getDefaultValue.asInstanceOf[java.lang.Long]
    new KafkaDataStore(broker, zk, exp)
  }

  override def createNewDataStore(params: ju.Map[String, Serializable]): DataStore = ???

  override def getDescription: String = ???

  override def getParametersInfo: Array[Param] = ???
}