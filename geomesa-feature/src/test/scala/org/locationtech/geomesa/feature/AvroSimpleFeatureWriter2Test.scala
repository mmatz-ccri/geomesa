package org.locationtech.geomesa.feature

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util.UUID

import com.vividsolutions.jts.geom.{Polygon, Point, Geometry}
import org.apache.avro.io.EncoderFactory
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ListBuffer
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureWriter2Test extends Specification {
  def createComplicatedFeatures(numFeatures : Int) : List[AvroSimpleFeature] = {
    val geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,f8:Polygon:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", geoSchema)
    val r = new Random()
    r.setSeed(0)


    val list = new ListBuffer[AvroSimpleFeature]
    for(i <- 0 until numFeatures){
      val fid = new FeatureIdImpl(r.nextString(5))
      val sf = new AvroSimpleFeature(fid, sft)

      sf.setAttribute("f0", r.nextString(10).asInstanceOf[Object])
      sf.setAttribute("f1", r.nextInt().asInstanceOf[Object])
      sf.setAttribute("f2", r.nextDouble().asInstanceOf[Object])
      sf.setAttribute("f3", r.nextFloat().asInstanceOf[Object])
      sf.setAttribute("f4", r.nextBoolean().asInstanceOf[Object])
      sf.setAttribute("f5", UUID.fromString("12345678-1234-1234-1234-123456789012"))
      sf.setAttribute("f6", new SimpleDateFormat("yyyyMMdd").parse("20140102"))
      sf.setAttribute("f7", GeohashUtils.wkt2geom("POINT(45.0 49.0)").asInstanceOf[Point])
      sf.setAttribute("f8", GeohashUtils.wkt2geom("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))").asInstanceOf[Polygon])
      list += sf
    }
    list.toList
  }

  "AvroSimpleFeatureWriter2" should {

    "correctly serialize a feature" in {
      val sft = SimpleFeatureTypes.createType("testType", "a:Integer,b:Date,*geom:Point:srid=4326")

      val f = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)
      f.setAttribute(0,"1")
      f.setAttribute(1,"2013-01-02T00:00:00.000Z")
      f.setAttribute(2,"POINT(45.0 49.0)")

      val oldBaos = new ByteArrayOutputStream()
      val oldbytes = Version1ASF(f).write(oldBaos)

      val afw = new AvroSimpleFeatureWriter2(sft)
      val newBaos = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().directBinaryEncoder(newBaos, null)
      afw.write(f, encoder)

      println(oldBaos.size())
      println(newBaos.size())

      success

    }

    "be faster" in {
      val features = createComplicatedFeatures(10000)

      val start = System.currentTimeMillis()
      val oldBaos = new ByteArrayOutputStream()
      features.foreach { f =>
        oldBaos.reset()
        Version1ASF(f).write(oldBaos)
      }
      println(System.currentTimeMillis() - start)


      val start2 = System.currentTimeMillis()
      val afw = new AvroSimpleFeatureWriter2(features(0).getType)
      val newBaos = new ByteArrayOutputStream()
      features.foreach { f =>
        newBaos.reset()
        val encoder = EncoderFactory.get().directBinaryEncoder(newBaos, null)
        afw.write(f, encoder)
      }
      println(System.currentTimeMillis() - start2)

      success
    }
  }

}
