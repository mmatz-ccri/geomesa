package org.locationtech.geomesa.feature

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.google.common.collect.Maps
import com.vividsolutions.jts.geom.Geometry
import org.apache.avro.Schema.Field
import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{EncoderFactory, Encoder, DatumWriter}
import org.apache.commons.codec.binary.Hex
import org.geotools.data.DataUtilities
import org.geotools.util.Converters
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.apache.avro.Schema.Type._
import scala.collection.JavaConversions._
import AvroSimpleFeatureWriter2._

class AvroSimpleFeatureWriter2(sft: SimpleFeatureType)
  extends DatumWriter[SimpleFeature] {

  private val schema: Schema = generateSchema(sft)

  override def setSchema(s: Schema): Unit = {}

  private val baos = new ByteArrayOutputStream()
  def encode(sf: SimpleFeature): Array[Byte] = {
    baos.reset()
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    write(sf, encoder)
    encoder.flush()
    baos.toByteArray
  }

  override def write(datum: SimpleFeature, out: Encoder): Unit = {

    def rawField(field: Field) = datum.getAttribute(field.pos() - 2)

    def getFieldValue[T](field: Field): T =
      if (rawField(field) == null) {
        null.asInstanceOf[T]
      } else {
        convertValue(field.pos - 2, rawField(field)).asInstanceOf[T]
      }

    def write(schema: Schema, f: Field): Unit = {
      schema.getType match {
        case UNION   =>
          val unionIdx = if (rawField(f) == null) 1 else 0
          out.writeIndex(unionIdx)
          write(schema.getTypes.get(unionIdx), f)
        case STRING  => out.writeString(getFieldValue[CharSequence](f))
        case BYTES   => out.writeBytes(getFieldValue[ByteBuffer](f))
        case INT     => out.writeInt(getFieldValue[Int](f))
        case LONG    => out.writeLong(getFieldValue[Long](f))
        case DOUBLE  => out.writeDouble(getFieldValue[Double](f))
        case FLOAT   => out.writeFloat(getFieldValue[Float](f))
        case BOOLEAN => out.writeBoolean(getFieldValue[Boolean](f))
        case NULL    => out.writeNull()
        case _ => throw new RuntimeException("unsupported avro simple feature type")
      }
    }

    // write first two
    out.writeInt(VERSION)
    out.writeString(datum.getID)

    schema.getFields.drop(2).foreach { f => write(f.schema(), f) }
  }

  val typeMap = createTypeMap(sft)
  val names = DataUtilities.attributeNames(sft).map(encodeAttributeName)

  def convertValue(idx: Int, v: AnyRef) = typeMap(names(idx)).conv.apply(v)
}

object AvroSimpleFeatureWriter2 {

  final val FEATURE_ID_AVRO_FIELD_NAME: String = "__fid__"
  final val AVRO_SIMPLE_FEATURE_VERSION: String = "__version__"
  final val VERSION: Int = 2
  final val AVRO_NAMESPACE: String = "org.geomesa"

  def encodeAttributeName(s: String): String = "_" + Hex.encodeHexString(s.getBytes("UTF8"))

  def decodeAttributeName(s: String): String = new String(Hex.decodeHex(s.substring(1).toCharArray), "UTF8")

  def generateSchema(sft: SimpleFeatureType): Schema = {
    val initialAssembler: SchemaBuilder.FieldAssembler[Schema] =
      SchemaBuilder.record(encodeAttributeName(sft.getTypeName))
        .namespace(AVRO_NAMESPACE)
        .fields
        .name(AVRO_SIMPLE_FEATURE_VERSION).`type`.intType.noDefault
        .name(FEATURE_ID_AVRO_FIELD_NAME).`type`.stringType.noDefault

    val result =
      sft.getAttributeDescriptors.foldLeft(initialAssembler) { case (assembler, ad) =>
        addField(assembler, encodeAttributeName(ad.getLocalName), ad.getType.getBinding, ad.isNillable)
      }

    result.endRecord
  }

  def addField(assembler: SchemaBuilder.FieldAssembler[Schema],
               name: String,
               ct: Class[_],
               nillable: Boolean): SchemaBuilder.FieldAssembler[Schema] = {
    val baseType = if (nillable) assembler.name(name).`type`.nullable() else assembler.name(name).`type`
    ct match {
      case c if classOf[String].isAssignableFrom(c)             => baseType.stringType.noDefault
      case c if classOf[java.lang.Integer].isAssignableFrom(c)  => baseType.intType.noDefault
      case c if classOf[java.lang.Long].isAssignableFrom(c)     => baseType.longType.noDefault
      case c if classOf[java.lang.Double].isAssignableFrom(c)   => baseType.doubleType.noDefault
      case c if classOf[java.lang.Float].isAssignableFrom(c)    => baseType.floatType.noDefault
      case c if classOf[java.lang.Boolean].isAssignableFrom(c)  => baseType.booleanType.noDefault
      case c if classOf[UUID].isAssignableFrom(c)               => baseType.bytesType.noDefault
      case c if classOf[Date].isAssignableFrom(c)               => baseType.longType.noDefault
      case c if classOf[Geometry].isAssignableFrom(c)           => baseType.bytesType.noDefault
    }
  }

  val primitiveTypes =
    List(
      classOf[String],
      classOf[java.lang.Integer],
      classOf[Int],
      classOf[java.lang.Long],
      classOf[Long],
      classOf[java.lang.Double],
      classOf[Double],
      classOf[java.lang.Float],
      classOf[Float],
      classOf[java.lang.Boolean],
      classOf[Boolean]
    )

  case class Binding(clazz: Class[_], conv: AnyRef => Any)

  def createTypeMap(sft: SimpleFeatureType) = {
    sft.getAttributeDescriptors.map { ad =>
      val conv =
        ad.getType.getBinding match {
          case t if primitiveTypes.contains(t) => (v: AnyRef) => v
          case t if classOf[UUID].isAssignableFrom(t) =>
            (v: AnyRef) => {
              val uuid = v.asInstanceOf[UUID]
              val bb = ByteBuffer.allocate(16)
              bb.putLong(uuid.getMostSignificantBits)
              bb.putLong(uuid.getLeastSignificantBits)
              bb.flip
              bb
            }

          case t if classOf[Date].isAssignableFrom(t) =>
            (v: AnyRef) => v.asInstanceOf[Date].getTime

          case t if classOf[Geometry].isAssignableFrom(t) =>
            (v: AnyRef) => ByteBuffer.wrap(WKBUtils.write(v.asInstanceOf[Geometry]))

          case _ =>
            (v: AnyRef) =>
              Option(Converters.convert(v, classOf[String])).getOrElse { a: AnyRef => a.toString }
        }

      (encodeAttributeName(ad.getLocalName), Binding(ad.getType.getBinding, conv))
    }.toMap
  }

}