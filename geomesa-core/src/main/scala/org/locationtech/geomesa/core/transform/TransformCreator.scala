package org.locationtech.geomesa.core.transform

import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.core.data.{FeatureEncoding, SimpleFeatureEncoderFactory}
import org.locationtech.geomesa.feature.{AvroSimpleFeature, AvroSimpleFeatureFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object TransformCreator {

  def createTransform(targetFeatureType: SimpleFeatureType,
                      featureEncoding: FeatureEncoding,
                      transformString: String): (SimpleFeature => Array[Byte]) =
    featureEncoding match {
      case FeatureEncoding.AVRO =>
        val encoder = SimpleFeatureEncoderFactory.createEncoder(targetFeatureType, featureEncoding)
        val defs = TransformProcess.toDefinition(transformString)
        (feature: SimpleFeature) => {
          val newSf = new AvroSimpleFeature(feature.getIdentifier, targetFeatureType)
          defs.map { t => newSf.setAttribute(t.name, t.expression.evaluate(feature)) }
          encoder.encode(newSf)
        }

      case FeatureEncoding.TEXT =>
        val defs = TransformProcess.toDefinition(transformString)
        val encoder = SimpleFeatureEncoderFactory.createEncoder(targetFeatureType, featureEncoding)
        val builder = AvroSimpleFeatureFactory.featureBuilder(targetFeatureType)
        (feature: SimpleFeature) => {
          builder.reset()
          defs.map { t => builder.set(t.name, t.expression.evaluate(feature)) }
          val newFeature = builder.buildFeature(feature.getID)
          encoder.encode(newFeature)
        }
    }

}
