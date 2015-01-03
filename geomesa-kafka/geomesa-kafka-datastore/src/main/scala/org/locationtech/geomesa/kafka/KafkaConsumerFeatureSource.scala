package org.locationtech.geomesa.kafka

import java.nio.charset.StandardCharsets
import java.util
import java.util.Properties
import java.util.concurrent.Executors

import com.google.common.eventbus.{EventBus, Subscribe}
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import com.vividsolutions.jts.index.quadtree.Quadtree
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.serializer.DefaultDecoder
import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, Query}
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.feature.AvroFeatureDecoder
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.IncludeFilter
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.identity.FeatureId
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Within}

import scala.collection.JavaConversions._

class KafkaConsumerFeatureSource(entry: ContentEntry,
                                 schema: SimpleFeatureType,
                                 eb: EventBus,
                                 producer: KafkaFeatureConsumer,
                                 query: Query)
  extends ContentFeatureStore(entry, query) {

  type FR = FeatureReader[SimpleFeatureType, SimpleFeature]
  val qt = new Quadtree
  val features = scala.collection.mutable.HashMap.empty[String, SimpleFeature]

  eb.register(this)

  @Subscribe
  def processNewFeatures(sf: SimpleFeature): Unit = {
    qt.insert(sf.point.getEnvelopeInternal, sf)
    features.put(sf.getID, sf)
  }

  @Subscribe
  def removeFeature(id: String): Unit = {
    features.remove(id).foreach { sf =>
      qt.remove(sf.point.getEnvelopeInternal, null)
    }
  }

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = schema

  override def getCountInternal(query: Query): Int =
    getReaderInternal(query).getIterator.size

  override def getReaderInternal(query: Query): FR =
    query.getFilter match {
      case i: IncludeFilter => include(i)
      case w: Within        => within(w)
      case b: BBOX          => bbox(b)
    }

  type DFR = DelegateFeatureReader[SimpleFeatureType, SimpleFeature]
  type DFI = DelegateFeatureIterator[SimpleFeature]
  def include(i: IncludeFilter) = new DFR(schema, new DFI(features.values.iterator))

  def within(w: Within): FR = {
    val (_, geomLit) = splitBinOp(w)
    val geom = geomLit.evaluate(null).asInstanceOf[Geometry]
    val res = qt.query(geom.getEnvelopeInternal)
    val filtered = res.asInstanceOf[java.util.List[SimpleFeature]].filter(sf => geom.contains(sf.point))
    val fiter = new DFI(filtered.iterator)
    new DFR(schema, fiter)
  }

  def bbox(b: BBOX): FR = {
    val bounds = JTS.toGeometry(b.getBounds)
    val res = qt.query(bounds.getEnvelopeInternal)
    val fiter = new DFI(res.asInstanceOf[java.util.List[SimpleFeature]].iterator)
    new DFR(schema, fiter)
  }

  def splitBinOp(binop: BinarySpatialOperator): (PropertyName, Literal) =
    binop.getExpression1 match {
      case pn: PropertyName => (pn, binop.getExpression2.asInstanceOf[Literal])
      case l: Literal       => (binop.getExpression2.asInstanceOf[PropertyName], l)
    }

  private var id = 1L
  def getNextId: FeatureId = {
    val ret = id
    id += 1
    new FeatureIdImpl(ret.toString)
  }

  override def getWriterInternal(query: Query, flags: Int) = throw new IllegalArgumentException("Not allowed")
}

trait FeatureProducer {
  def eventBus: EventBus
  def produceFeatures(f: SimpleFeature): Unit = eventBus.post(f)
  def deleteFeature(id: String): Unit = eventBus.post(id)
  def deleteFeatures(ids: Seq[String]): Unit = ids.foreach(deleteFeature)
}

class KafkaFeatureConsumer(topic: String, 
                           zookeepers: String, 
                           groupId: String,
                           featureDecoder: AvroFeatureDecoder,
                           override val eventBus: EventBus) extends FeatureProducer {

  private val client = Consumer.create(new ConsumerConfig(buildClientProps))
  private val whiteList = new Whitelist(topic)
  private val decoder: DefaultDecoder = new DefaultDecoder(null)
  private val stream =
    client.createMessageStreamsByFilter(whiteList, 1, decoder, decoder).head

  val es = Executors.newSingleThreadExecutor()
  es.submit(new Runnable {
    override def run(): Unit = {
      val iter = stream.iterator()
      while (iter.hasNext) {
        val msg = iter.next()
        if(msg.key() != null && util.Arrays.equals(msg.key(), KafkaProducerFeatureStore.DELETE_KEY)) {
          val id = new String(msg.message(), StandardCharsets.UTF_8)
          deleteFeature(id)
        } else {
          val f = featureDecoder.decode(msg.message())
          produceFeatures(f)
        }
      }
    }
  })

  private def buildClientProps = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeepers)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "2000")
    props.put("zookeeper.sync.time.ms", "1000")
    props.put("auto.commit.interval.ms", "1000")
    props
  }
}
