package org.locationtech.geomesa.tools.commands

import java.nio.ByteBuffer
import java.util.regex.Pattern

import com.beust.jcommander.{IStringConverter, IStringConverterFactory}
import org.locationtech.geomesa.tools.commands.Converters.JPatternConverter

object Converters {

  class JPatternConverter extends IStringConverter[Pattern] {
    override def convert(value: String): Pattern = Pattern.compile(value)
  }
}

class GeoMesaIStringConverterFactory extends IStringConverterFactory {
  import org.locationtech.geomesa.tools.commands.GeoMesaIStringConverterFactory.ConverterMap

  override def getConverter[T](forType: Class[T]): Option[Class[_ <: IStringConverter[_]]] =
    ConverterMap.get(forType)
}

object GeoMesaIStringConverterFactory {
  val ConverterMap: Map[Class[_], Class[_ <: IStringConverter[_]]] =
    Map[Class[_], Class[_ <: IStringConverter[_]]](
      classOf[Pattern] -> classOf[JPatternConverter]
    )
}

class Image {

  var Complex = null
  var data: Array[Double] = null
  var data2: Array[Int] = null
  var meta1: String = null
  var meta2: Double = null

  def writeTo(buf :ByteBuffer) = {
    buf.put(data2.length)
    buf.put(data2)

    var arr = complex.toByteArray()
    buf.put(arr.len)
    buf.put(arr)

  }

  read(buf: Buffer) {
    data2len = buf.getInt()
    fo
  }
}