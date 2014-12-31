package org.locationtech.geomesa.tools.commands

import java.util.regex.Pattern

import com.beust.jcommander.{IStringConverter, IStringConverterFactory}
import org.locationtech.geomesa.tools.commands.Converters.JPatternConverter
import GeoMesaIStringConverterFactory._

class GeoMesaIStringConverterFactory extends IStringConverterFactory {
  override def getConverter[T](forType: Class[T]): Option[Class[_ <: IStringConverter[_]]] =
  ConverterMap.get(forType)
}

object GeoMesaIStringConverterFactory {
  val ConverterMap: Map[Class[_], Class[_ <: IStringConverter[_]]] =
    Map[Class[_], Class[_ <: IStringConverter[_]]](
      classOf[Pattern] -> classOf[JPatternConverter]
    )
}
