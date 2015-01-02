package org.locationtech.geomesa.kafka

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
import org.opengis.filter.identity.FeatureId

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

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    new FW {
      val encoder = new AvroFeatureEncoder(schema)
      val props = new Properties()
      props.put("metadata.broker.list", broker)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")
      val kafkaProducer = new Producer[String, Array[Byte]](new ProducerConfig(props))

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

  }

  override def getCountInternal(query: Query): Int = 0
  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = null
}
