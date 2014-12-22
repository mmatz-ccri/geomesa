package org.locationtech.geomesa.utils.unix

import scala.collection.mutable
import scala.util.Try
import scala.util.parsing.combinator.RegexParsers

import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.io.IOUtils
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime

object DSL {


  trait ProcessingFunction {
    def evaluate(args: Array[AnyRef]): AnyRef
  }

  def toInt(arg: Any): java.lang.Integer = arg match {
    case a: Int    => a
    case a: String => a.toInt
  }

  def toDouble(arg: Any): java.lang.Double = arg match {
    case a: Double => a
    case a: String => a.toDouble
  }


  sealed trait Expr {
    def evaluate(cols: Array[AnyRef]): AnyRef
  }

  case class Lit(l: String) extends Expr {
    override def evaluate(cols: Array[AnyRef]): String = l
  }
  case class Col(i: Int) extends Expr {
    override def evaluate(cols: Array[AnyRef]): AnyRef = cols(i)
  }
  case class ColAssignment(col: Col, e: Expr) extends Expr {
    override def evaluate(cols: Array[AnyRef]): Array[AnyRef] = {
      (cols.toArray)(col.i) = e.evaluate(cols)
      cols.toArray
    }
  }
  case class Fn(fn: ProcessingFunction, args: Seq[Expr]) extends Expr {
    override def evaluate(cols: Array[AnyRef]): AnyRef = {
      fn.evaluate(args.map(_.evaluate(cols)).toArray)
    }
  }
  case class RegexPattern(pattern: String) extends Expr {
    private val r = pattern.r
    override def evaluate(cols: Array[AnyRef]): AnyRef = r.findFirstMatchIn(cols(0).toString)
  }

  val fnRegistry = mutable.HashMap.empty[String, ProcessingFunction]
  def registerFn[T](name: String, f: ProcessingFunction) = fnRegistry.put(name, f)

  implicit def f2ProcessingFunction[T](f: Array[AnyRef] => AnyRef): ProcessingFunction =
    new ProcessingFunction {
      override def evaluate(args: Array[AnyRef]): AnyRef = f(args)
    }

  registerFn("+",       (args: Array[AnyRef]) => Integer.valueOf(toInt(args(0)) + toInt(args(1))))
  registerFn("toInt",   (args: Array[AnyRef]) => toInt(args(0)))
  registerFn("toDouble",(args: Array[AnyRef]) => toDouble(args(0)))
  registerFn("concat",  (args: Array[AnyRef]) => args(0).toString + args(1).toString)
  registerFn("length",  (args: Array[AnyRef]) => toInt(args(0).toString.length))
  registerFn("find",    (args: Array[AnyRef]) => Integer.valueOf(args(0).toString.indexOf(args(1).toString)))
  registerFn("min",     (args: Array[AnyRef]) => Integer.valueOf(math.min(toInt(args(0)), toInt(args(1)))))
  registerFn("toDate",  (args: Array[AnyRef]) => new DateTime(args(0)))
  registerFn("extract", (args: Array[AnyRef]) => args(0).toString match {
    case "hours" => Integer.valueOf(args(1).asInstanceOf[DateTime].hourOfDay().get())
    case "minutes"  => Integer.valueOf(args(1).asInstanceOf[DateTime].minuteOfHour().get())
  })
  private val gf = JTSFactoryFinder.getGeometryFactory
  registerFn("point",   (args: Array[AnyRef]) => gf.createPoint(new Coordinate(toDouble(args(0)), toDouble(args(1)))))

  object AwkParser extends RegexParsers {
    def lit = "[^$,()]+".r ^^ {
      case i => Lit(i)
    }

    def col    = "$" ~> "[0-9]+".r ^^ {
      case i => Col(i.toInt - 1)
    }

    def colAssignment: Parser[Expr] = col ~ "=" ~ expr ^^ {
      case i ~ "=" ~ expr => ColAssignment(i, expr)
    }

    def atom = colAssignment | col | lit
    def func: Parser[Expr] = "[^()$,]*".r ~ "(" ~ rep1sep(expr, ",") ~ ")" ^^ {
      case name ~ "(" ~ args ~ ")" => Fn(fnRegistry(name), args)
    }
    def expr   = func | atom
    def exprs  = rep1sep(expr, ";")
    def parse(s: String): Seq[Expr] = parse(exprs, s).get
  }
  class FlowProcessor {
    outer =>

    def awk(expr: String): FlowProcessor = {
      new FlowProcessor {
        val expressions = AwkParser.parse(expr)

        override def run(args: Iterator[Array[AnyRef]]): Iterator[Array[AnyRef]] = {
          val processed = outer.run(args)
          processed.flatMap { line =>
            Try {
              expressions.map(_.evaluate(line)).toArray
            }.toOption
          }
        }
      }
    }

    def run(obj: Iterator[Array[AnyRef]]): Iterator[Array[AnyRef]] = obj
  }

  import scala.collection.JavaConversions._
  def main(args: Array[String]) {
    val data =
      """
        |1,2,hello,world,2011-01-01T00:00:00.000Z
        |3,4,hello,foo,2011-01-01T12:00:00.000Z
        |5,6,bar,baz,2011-01-01T13:00:00.000Z
        |""".stripMargin

    val is = IOUtils.readLines(IOUtils.toInputStream(data)).iterator().map(_.asInstanceOf[String].split(",")).asInstanceOf[Iterator[Array[AnyRef]]]

    val transformed =
      new FlowProcessor()
        .awk("min(extract(minutes,toDate($5)),extract(hours,toDate($5)));$1;+($1,toInt($2));$3;$4;concat($3,$4);concat(myword,$4);$1;$2")
        .awk("$5;find($4,ell);point($8,$9)")

    val l = transformed.run(is).toList
    l.foreach { t => println(t.map(_.toString).mkString(",")) }
  }
}
