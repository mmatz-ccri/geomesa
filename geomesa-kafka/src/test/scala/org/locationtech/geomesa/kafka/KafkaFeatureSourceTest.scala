package org.locationtech.geomesa.kafka

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.eventbus.EventBus
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.geotools.data.store.ContentEntry
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.NameImpl
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.{JTSFactoryFinder, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.util.SftBuilder
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class KafkaFeatureSourceTest extends Specification with Logging {

  sequential

  class TestFeatureProducer(override val eventBus: EventBus, schema: SimpleFeatureType) extends FeatureProducer {
    val builder = new SimpleFeatureBuilder(schema)
    val gf = JTSFactoryFinder.getGeometryFactory
    var id = 1L
    def produceTestFeatures(n: Int): Seq[SimpleFeature] = {
      val features = (0 until n).map { _ =>
        builder.reset()
        builder.addAll(Array("foo", 10, randomPoint).asInstanceOf[Array[AnyRef]])
        val f = builder.buildFeature(id.toString)
        id += 1
        f
      }
      produceFeatures(DataUtilities.collection(features))
      features
    }

    def randomPoint: Point = gf.createPoint(new Coordinate(randomLon, randomLat))

    def randomLon: Double = -180.0 + 360 * Random.nextDouble()
    def randomLat: Double = -90.0 + 180 * Random.nextDouble()
  }

  "KafkaFeatureSource" should {
    val testSchema = new SftBuilder().stringType("name").intType("age").point("geom").build("testfeature")
    val eb = new EventBus()
    val producer = new TestFeatureProducer(eb, testSchema)
    val ds = new KafkaDataStore(null, null, 1L)
    val fs = new KafkaFeatureSource(new ContentEntry(ds, new NameImpl("testfeature")), testSchema, eb, producer, null, null, 10L, null)
    val ff = CommonFactoryFinder.getFilterFactory2

    "allow features to be added" >> {
      val features = producer.produceTestFeatures(10)
      val res = fs.getCount(Query.ALL)
      res must be equalTo 10

      val f = features.head
      val loc = f.point.buffer(0.0001)

      "and subsequently queried using within clauses" >> {
        val spatialFilter = ff.within(ff.property("geom"), ff.literal(loc))
        val queryRes = fs.getFeatures(spatialFilter)
        queryRes.size() must be greaterThanOrEqualTo 1
        queryRes.features().map(_.getID) must contain(f.getID)
      }

      "and with bbox clauses" >> {
        val bbox = new ReferencedEnvelope(loc.getEnvelopeInternal, DefaultGeographicCRS.WGS84)
        val spatialFilter = ff.bbox(ff.property("geom"), bbox)
        val queryRes = fs.getFeatures(spatialFilter)
        queryRes.size() must be greaterThanOrEqualTo 1
        queryRes.features().map(_.getID) must contain(f.getID)
      }

      "flush should clear out old features" >> {
        val cutoff = DateTime.now().getMillis
        producer.produceTestFeatures(5)
        fs.flush(cutoff)
        val res = fs.getCount(Query.ALL)
        res must be equalTo 5
      }

      "multi-threaded access" >> {
        val queryers = Executors.newScheduledThreadPool(2)
        val flushSched = Executors.newScheduledThreadPool(1)
        flushSched.scheduleAtFixedRate(new Runnable {
          var last = DateTime.now().getMillis
          override def run(): Unit = {
            println(s"Flushing older than $last")
            fs.flush(last)
            last = DateTime.now().getMillis
          }
        }, 1, 3, TimeUnit.SECONDS)

        val r = new Runnable {
          override def run(): Unit = println(fs.getCount(Query.ALL))
        }
        queryers.scheduleAtFixedRate(r, 0, 2, TimeUnit.SECONDS)
        queryers.scheduleAtFixedRate(r, 1, 2, TimeUnit.SECONDS)

        var count = 0
        val producerThread = Executors.newScheduledThreadPool(1)
        val producerRunnable = new Runnable {
          override def run(): Unit = {
            println("Producing features")
            producer.produceTestFeatures(1000000)
            count += 1
            if(count < 10) producerThread.schedule(this, Random.nextInt(5), TimeUnit.SECONDS)
          }
        }

        producerThread.schedule(producerRunnable, Random.nextInt(5), TimeUnit.SECONDS)

        Thread.sleep(90*1000)
        producerThread.shutdown()
        queryers.shutdown()
        flushSched.shutdown()

        1 must be equalTo 1
      }
    }

  }
}
