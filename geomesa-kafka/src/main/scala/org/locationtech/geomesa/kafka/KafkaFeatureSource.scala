package org.locationtech.geomesa.kafka

import java.util.Properties
import java.util.concurrent.Executors

import com.google.common.collect._
import com.google.common.eventbus.{EventBus, Subscribe}
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import com.vividsolutions.jts.index.quadtree.Quadtree
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.FeatureCollection
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.IncludeFilter
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.identity.FeatureId
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Within}

import scala.collection.JavaConversions._
import scala.collection.mutable

class KafkaFeatureSource(entry: ContentEntry,
                         schema: SimpleFeatureType,
                         eb: EventBus,
                         producer: FeatureProducer,
                         zookeepers: String,
                         groupId: String,
                         exp: Long,
                         query: Query)
  extends ContentFeatureStore(entry, query) {

  type FR = FeatureReader[SimpleFeatureType, SimpleFeature]
  val qt = new Quadtree
  val features = Queues.newConcurrentLinkedQueue[FeatureHolder]()

  eb.register(this)

  case class FeatureHolder(l: Long, sf: SimpleFeature)

  def flush(cutoff: Long): Unit = {
    var e = features.peek()
    while(e != null && e.l < cutoff) {
      features.poll()
      qt.remove(e.sf.point.getEnvelopeInternal, e.sf)
      e = features.peek()
    }
  }

  @Subscribe
  def processNewFeatures(coll: FeatureCollection[SimpleFeatureType, SimpleFeature]): Unit =
    addFeatures(coll)


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
  def include(i: IncludeFilter) = new DFR(schema, new DFI(features.toArray.map(_.asInstanceOf[FeatureHolder].sf).iterator))

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

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] =
    new FeatureWriter[SimpleFeatureType, SimpleFeature] {
      var sf: SimpleFeature = null
      var saved = mutable.Buffer.empty[FeatureHolder]
      override def getFeatureType: SimpleFeatureType = schema
      override def next(): SimpleFeature = {
        if(sf != null) write()
        sf = new AvroSimpleFeature(getNextId, schema)
        sf
      }
      override def remove(): Unit = ???
      override def hasNext: Boolean = false
      override def write(): Unit = {
        if(sf != null) saved += FeatureHolder(DateTime.now().getMillis, sf)
        sf = null
      }
      override def close(): Unit = {
        // write out all the features
        saved.foreach { fh =>
          qt.insert(fh.sf.point.getEnvelopeInternal, fh.sf)
          features.add(fh)
        }
      }
    }
}

trait FeatureProducer {
  def eventBus: EventBus
  def produceFeatures(coll: FeatureCollection[SimpleFeatureType, SimpleFeature]): Unit =
    eventBus.post(coll)
}

class KafkaFeatureConsumer(topic: String, 
                           zookeepers: String, 
                           groupId: String, 
                           override val eventBus: EventBus) extends FeatureProducer {
  val client = Consumer.create(new ConsumerConfig(buildClientProps))
  val whiteList = new Whitelist(topic)
  val stream = client.createMessageStreamsByFilter(whiteList).head

  val es = Executors.newSingleThreadExecutor()
  es.submit(new Runnable {
    override def run(): Unit = {

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
