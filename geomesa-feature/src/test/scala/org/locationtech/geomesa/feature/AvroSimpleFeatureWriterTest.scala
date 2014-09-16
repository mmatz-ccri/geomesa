package org.locationtech.geomesa.feature

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util.UUID

import com.vividsolutions.jts.geom.{Point, Polygon}
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
class AvroSimpleFeatureWriterTest extends Specification {
  def createComplicatedFeatures(numFeatures : Int) : List[Version2ASF] = {
    val geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,f8:Polygon:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", geoSchema)
    val r = new Random()
    r.setSeed(0)


    val list = new ListBuffer[Version2ASF]
    for(i <- 0 until numFeatures){
      val fid = new FeatureIdImpl(r.nextString(5))
      val sf = new Version2ASF(fid, sft)

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
      val oldbytes = Version2ASF(f).write(oldBaos)

      val afw = new AvroSimpleFeatureWriter(sft)
      val newBaos = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().directBinaryEncoder(newBaos, null)
      afw.write(f, encoder)
      encoder.flush()

      oldBaos.toByteArray mustEqual newBaos.toByteArray
    }

  }

}
