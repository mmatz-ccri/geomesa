package org.locationtech.geomesa.utils.geotools

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SpecBuilderTest extends Specification {

  sequential

  "SpecBuilder" >> {
    "build simple types" >> {
      val spec = new SpecBuilder().intType("i").longType("l").floatType("f").doubleType("d").stringType("s").toString()
      spec mustEqual "i:Integer,l:Long,f:Float,d:Double,s:String"
    }

    "handle date and uuid types" >> {
      val spec = new SpecBuilder().date("d").uuid("u").toString()
      spec mustEqual "d:Date,u:UUID"
    }

    "provide index when set to true" >> {
      val spec = new SpecBuilder()
        .intType("i",true)
        .longType("l",true)
        .floatType("f",true)
        .doubleType("d",true)
        .stringType("s",true)
        .date("dt",true)
        .uuid("u",true)
        .toString()
      val expected = "i:Integer,l:Long,f:Float,d:Double,s:String,dt:Date,u:UUID".split(",").map(_+":index=true").mkString(",")
      spec mustEqual expected
    }

    "configure table splitters properly" >> {
      val sft = new SpecBuilder()
        .intType("i")
        .longType("l")
        .recordSplitter("org.locationtech.geomesa.core.data.DigitSplitter", Map("fmt" ->"%02d", "min" -> "0", "max" -> "99"))
        .buildSFT("test")

      sft.getAttributeCount mustEqual 2
      sft.getAttributeDescriptors.map(_.getLocalName) must containAllOf( List("i", "l"))

      sft.getUserData.get(SimpleFeatureTypes.TABLE_SPLITTER) must be equalTo "org.locationtech.geomesa.core.data.DigitSplitter"
      val opts = sft.getUserData.get(SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS).asInstanceOf[Map[String, String]]
      opts.size must be equalTo 3
      opts("fmt") must be equalTo "%02d"
      opts("min") must be equalTo "0"
      opts("max") must be equalTo "99"
    }

    // Example of fold...also can do more complex things like zipping to automatically build SFTs
    "work with foldLeft" >> {
      val spec = ('a' to 'z').foldLeft(new SpecBuilder()) { case (builder, name) =>
        builder.stringType(name.toString)
      }

      val expected = ('a' to 'z').map{ c => c.toString + ":" + "String" }.mkString(",")
      spec.toString() mustEqual expected

      val sft = spec.buildSFT("foobar")
      sft.getAttributeCount mustEqual 26
      sft.getAttributeDescriptors.map(_.getLocalName).toList mustEqual ('a' to 'z').map(_.toString).toList
    }

  }
}
