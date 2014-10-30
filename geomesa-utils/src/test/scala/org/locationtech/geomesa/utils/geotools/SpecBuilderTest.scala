package org.locationtech.geomesa.utils.geotools

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpecBuilderTest extends Specification {

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
  }
}
