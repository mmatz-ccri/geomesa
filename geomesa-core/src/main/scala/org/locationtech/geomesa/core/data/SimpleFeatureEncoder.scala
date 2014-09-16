/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.data

import java.io.{ByteArrayInputStream, InputStream}

import org.apache.accumulo.core.data.{Value => AValue}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.{AvroSimpleFeatureWriter, FeatureSpecificReader}
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


/**
 * Responsible for collecting data-entries that share an identifier, and
 * when done, collapsing these rows into a single SimpleFeature.
 *
 * All encoding/decoding/serializing of features should be done through this
 * single class to allow future versions of serialization instead of scattering
 * knowledge of how the serialization is done through the org.locationtech.geomesa.core codebase
 */

trait SimpleFeatureEncoder {
  def encode(feature: SimpleFeature) : Array[Byte]
  def decode(featureValue: AValue) : SimpleFeature
  def extractFeatureId(value: AValue): String
  def getName = getEncoding.toString
  def getEncoding: FeatureEncoding
}

object FeatureEncoding extends Enumeration {
  type FeatureEncoding = Value
  val AVRO = Value("avro").asInstanceOf[FeatureEncoding]
  val TEXT = Value("text").asInstanceOf[FeatureEncoding]
}

class TextFeatureEncoder(sft: SimpleFeatureType) extends SimpleFeatureEncoder{
  def encode(feature:SimpleFeature) : Array[Byte] =
    ThreadSafeDataUtilities.encodeFeature(feature).getBytes()

  def decode(featureValue: AValue) = {
    ThreadSafeDataUtilities.createFeature(sft, featureValue.toString)
  }

  // This is derived from the knowledge of the GeoTools encoding in DataUtilities
  def extractFeatureId(value: AValue): String = {
    val vString = value.toString
    vString.substring(0, vString.indexOf("="))
  }

  override def getEncoding: FeatureEncoding = FeatureEncoding.TEXT
}

/**
 * This could be done more cleanly, but the object pool infrastructure already
 * existed, so it was quickest, easiest simply to abuse it.
 */
object ThreadSafeDataUtilities {
  private[this] val dataUtilitiesPool = ObjectPoolFactory(new Object, 1)

  def encodeFeature(feature:SimpleFeature) : String = dataUtilitiesPool.withResource {
    _ => DataUtilities.encodeFeature(feature)
  }

  def createFeature(simpleFeatureType:SimpleFeatureType, featureString:String) : SimpleFeature =
    dataUtilitiesPool.withResource {
      _ => DataUtilities.createFeature(simpleFeatureType, featureString)
    }
}

/**
 * Encode features as avro making reuse of binary decoders and encoders
 * as well as a custom datum writer and reader
 *
 * This class is NOT threadsafe and cannot be across multiple threads.
 *
 * @param sft
 */
class AvroFeatureEncoder(sft: SimpleFeatureType) extends SimpleFeatureEncoder {

  private val writer = new AvroSimpleFeatureWriter(sft)
  private val reader = FeatureSpecificReader(sft)
  private var decoder: BinaryDecoder = null

  def encode(feature: SimpleFeature): Array[Byte] = writer.encode(feature)

  def decode(featureAValue: AValue) = decode(new ByteArrayInputStream(featureAValue.get()))

  def decode(is: InputStream) = {
    decoder = DecoderFactory.get().binaryDecoder(is, decoder)
    reader.read(null, decoder)
  }

  def extractFeatureId(aValue: AValue) =
    FeatureSpecificReader.extractId(new ByteArrayInputStream(aValue.get()), decoder)

  override def getEncoding: FeatureEncoding = FeatureEncoding.AVRO
}

