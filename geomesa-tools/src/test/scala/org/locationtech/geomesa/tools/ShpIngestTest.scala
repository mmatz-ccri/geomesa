package org.locationtech.geomesa.tools

import java.io.File
import java.util.Date

import com.google.common.io.Files
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.Transaction
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.DataStoreStuff
import org.locationtech.geomesa.tools.commands.IngestCommand.IngestParameters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShpIngestTest extends Specification {

  import org.locationtech.geomesa.utils.geotools.Conversions._

import scala.collection.JavaConversions._

  sequential

  "ShpIngest" >> {
    val geomBuilder = JTSFactoryFinder.getGeometryFactory

    val shpStoreFactory = new ShapefileDataStoreFactory
    val shpFile = new File(Files.createTempDir(), "shpingest.shp")
    val shpUrl = shpFile.toURI.toURL
    val params = Map("url" -> shpUrl)
    val shpStore = shpStoreFactory.createNewDataStore(params)
    val schema = SimpleFeatureTypes.createType("shpingest", "age:Integer,dtg:Date,*geom:Point:srid=4326")
    shpStore.createSchema(schema)
    val data =
      List(
        ("1", 1, new Date(), (10.0, 10.0)),
        ("1", 2, new Date(), (20.0, 20.0))
      )
    val writer = shpStore.getFeatureWriterAppend("shpingest", Transaction.AUTO_COMMIT)
    data.foreach { case (id, age, dtg, (lat, lon)) =>
      val f = writer.next()
      f.setAttribute("age", age)
      f.setAttribute("dtg", dtg)
      val pt = geomBuilder.createPoint(new Coordinate(lat, lon))
      f.setDefaultGeometry(pt)
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      f.getUserData.put(Hints.PROVIDED_FID, id)
      writer.write()
    }
    writer.flush()
    writer.close()

    val ingestParams = new IngestParameters()
    ingestParams.instance = "mycloud"
    ingestParams.zookeepers = "zoo1,zoo2,zoo3"
    ingestParams.user = "myuser"
    ingestParams.password = "mypassword"
    ingestParams.catalog = "testshpingestcatalog"
    ingestParams.useMock = true

    val ds = new DataStoreStuff(ingestParams).ds

    "should properly ingest a shapefile" >> {
      ingestParams.file = shpFile
      new ShpIngest(ingestParams).run()

      val fs = ds.getFeatureSource("shpingest")
      val result = fs.getFeatures.features().toList
      result.length mustEqual 2
    }

    "should support renaming the feature type" >> {
      ingestParams.featureName = "changed"
      new ShpIngest(ingestParams).run()

      val fs = ds.getFeatureSource("changed")
      val result = fs.getFeatures.features().toList
      result.length mustEqual 2
    }
  }
}
