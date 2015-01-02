package org.locationtech.geomesa.kafka

import java.nio.charset.StandardCharsets
import java.util.Properties

import com.vividsolutions.jts.geom.Envelope
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.feature.{AvroFeatureEncoder, AvroSimpleFeature}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Id, Filter}
import org.opengis.filter.identity.FeatureId

import scala.collection.JavaConversions._

class KafkaProducerFeatureStore(entry: ContentEntry,
                                schema: SimpleFeatureType,
                                broker: String,
                                query: Query)
  extends ContentFeatureStore(entry, query) {

  val typeName = entry.getTypeName

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = schema

  type FW = FeatureWriter[SimpleFeatureType, SimpleFeature]


  override def removeFeatures(filter: Filter): Unit = super.removeFeatures(filter)

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] =
    if(query != null) new DeletingFeatureWriter(query)
    else new AppendingFeatureWriter

  trait KafkaFeatureWriter {
    val encoder = new AvroFeatureEncoder(schema)
    val props = new Properties()
    props.put("metadata.broker.list", broker)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val kafkaProducer = new Producer[String, Array[Byte]](new ProducerConfig(props))
  }

  class AppendingFeatureWriter extends FW with KafkaFeatureWriter {
    var sf: SimpleFeature = null
    private var id = 1L
    def getNextId: FeatureId = {
      val ret = id
      id += 1
      new FeatureIdImpl(ret.toString)
    }
    override def getFeatureType: SimpleFeatureType = schema
    override def next(): SimpleFeature = {
      sf = new AvroSimpleFeature(getNextId, schema)
      sf
    }
    override def remove(): Unit = {}
    override def hasNext: Boolean = false

    override def write(): Unit = {
      val encoded = encoder.encode(sf)
      val msg = new KeyedMessage[String, Array[Byte]](typeName, encoded)
      sf = null
      kafkaProducer.send(msg)
    }
    override def close(): Unit = {}
  }

  class DeletingFeatureWriter(query: Query) extends FW with KafkaFeatureWriter {
    val toDelete =
      if(query == null) Seq()
      else query match {
        case ids: Id => ids.getIDs.toIndexedSeq
        case _       => Seq()
      }
    val toDeleteIter = toDelete.iterator

    var curDel: String = null
    val EMPTY_SF = new AvroSimpleFeature(null, schema)
    override def getFeatureType: SimpleFeatureType = ???

    override def next(): SimpleFeature = {
      curDel = toDeleteIter.next().asInstanceOf[String]
      EMPTY_SF.getIdentifier.asInstanceOf[FeatureIdImpl].setID(curDel)
      EMPTY_SF
    }

    private val DELETE_KEY = "delete"
    override def remove(): Unit = {
      val bytes = curDel.getBytes(StandardCharsets.UTF_8)
      val delMsg = new KeyedMessage[String, Array[Byte]](typeName, DELETE_KEY, bytes)
      kafkaProducer.send(delMsg)
    }

    override def write(): Unit = {}
    override def hasNext: Boolean = toDeleteIter.hasNext
    override def close(): Unit = {}
  }

  override def getCountInternal(query: Query): Int = 0
  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = null
}
