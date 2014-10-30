package org.locationtech.geomesa.utils.geotools

import java.util.{Date, UUID}

import org.locationtech.geomesa.utils.geotools.SpecBuilder._

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.{Type => UType, _}


class SpecBuilder {

  private val entries = new ListBuffer[String]

  // Primitives
  def stringType(name: String, index: Boolean = false): SpecBuilder  = append(name, index, "String")
  def intType(name: String, index: Boolean = false): SpecBuilder     = append(name, index, "Integer")
  def longType(name: String, index: Boolean = false): SpecBuilder    = append(name, index, "Long")
  def floatType(name: String, index: Boolean = false): SpecBuilder   = append(name, index, "Float")
  def doubleType(name: String, index: Boolean = false): SpecBuilder  = append(name, index, "Double")
  def booleanType(name: String, index: Boolean = false): SpecBuilder = append(name, index, "Boolean")

  // Helpful Types
  def date(name: String, index: Boolean = false): SpecBuilder = append(name, index, "Date")
  def uuid(name: String, index: Boolean = false): SpecBuilder = append(name, index, "UUID")

  // Single Geometries
  def point(name: String, default: Boolean = false, index: Boolean = false): SpecBuilder =
    appendGeom(name, index, default, "Point")
  def lineString(name: String, default: Boolean = false, index: Boolean = false): SpecBuilder =
    appendGeom(name, index, default, "LineString")
  def polygon(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "Polygon")
  def geometry(name: String, default: Boolean = false, index: Boolean = false): SpecBuilder =
    appendGeom(name, index, default, "Geometry")

  // Multi Geometries
  def multiPoint(name: String, default: Boolean = false, index: Boolean = false): SpecBuilder =
    appendGeom(name, index, default, "MultiPoint")
  def multiLineString(name: String, default: Boolean = false, index: Boolean = false): SpecBuilder =
    appendGeom(name, index, default, "MultiLineString")
  def multiPolygon(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "MultiPolygon")
  def geometryCollection(name: String, default: Boolean = false, index: Boolean = false): SpecBuilder =
    appendGeom(name, index, default, "GeometryCollection")

  // List and Map Types
  def mapType[K: TypeTag, V: TypeTag](name: String, index: Boolean = false) =
    append(name, index, s"Map[${resolve(typeOf[K])},${resolve(typeOf[V])}]")
  def listType[T: TypeTag](name: String, index: Boolean = false) = append(name, index, s"List[${resolve(typeOf[T])}]")

  private def resolve(tt: UType): String =
    tt match {
      case t if primitiveTypes.contains(tt) => tt.toString
      case t if tt == typeOf[Date]          => "Date"
      case t if tt == typeOf[UUID]          => "UUID"
    }

  private def append(name: String, index: Boolean, typeStr: String): SpecBuilder = {
    val parts = List(name, typeStr) ++ indexPart(index)
    entries += parts.mkString(SepPart)
    this
  }

  private def appendGeom(name: String, index: Boolean, default: Boolean, typeStr: String): SpecBuilder = {
    val namePart  = if (default) "*" + name else name
    val parts     = List(namePart, typeStr, SridPart) ++ indexPart(index)
    entries += parts.mkString(SepPart)
    this
  }

  private def indexPart(index: Boolean) = if (index) Some("index=true") else None

  override def toString() = entries.mkString(SepEntry)

}

object SpecBuilder {

  val SridPart = "srid=4326"
  val SepPart  = ":"
  val SepEntry = ","

  val primitiveTypes =
    List(
      typeOf[String],
      typeOf[java.lang.Integer],
      typeOf[Int],
      typeOf[java.lang.Long],
      typeOf[Long],
      typeOf[java.lang.Double],
      typeOf[Double],
      typeOf[java.lang.Float],
      typeOf[Float],
      typeOf[java.lang.Boolean],
      typeOf[Boolean]
    )


  def main(args: Array[String]) = {
    val spec = new SpecBuilder()
      .stringType("foobar")
      .stringType("baz", index=true)
      .longType("time")
      .mapType[UUID,String]("mymap", index=false)
      .listType[String]("intList")
      .point("geom", default=true)

    println(spec) // foobar:String,baz:String:index=true,time:Long,mymap:Map[UUID,String],intList:List[String],*geom:Point:srid=4326

    val sft = SimpleFeatureTypes.parse(spec.toString())
    println(sft.attributes.map(_.name).mkString(", ")) // foobar, baz, time, mymap, intList, geomgit

  }
}

