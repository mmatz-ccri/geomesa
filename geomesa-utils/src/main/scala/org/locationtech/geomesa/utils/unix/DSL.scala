package org.locationtech.geomesa.utils.unix

import java.security.MessageDigest

import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime

import scala.collection.mutable
import scala.util.Try
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

object DSL {


  trait ProcessingFunction {
    def name: String
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
    def combine(acc: Array[AnyRef], o: AnyRef) = acc.:+(o)
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
  case class ColRange(s: Int, e: Int) extends Expr {
    override def evaluate(cols: Array[AnyRef]): Array[AnyRef] = (s to e).map(cols.apply).toArray

    override def combine(acc: Array[AnyRef], o: AnyRef): Array[AnyRef] = acc ++ o.asInstanceOf[Array[AnyRef]]
  }
  case class Fn(fn: ProcessingFunction, args: Seq[Expr]) extends Expr {
    override def evaluate(cols: Array[AnyRef]): AnyRef = {
      fn.evaluate(args.foldLeft(Array.empty[AnyRef]) { (acc, expr) =>
        expr.combine(acc, expr.evaluate(cols))
      })
    }
  }
  case class Apply(fn: ProcessingFunction, args: Seq[Expr]) extends Expr {
    override def evaluate(cols: Array[AnyRef]): AnyRef = {
      val evaluated = args.foldLeft(Array.empty[AnyRef]) { (acc, expr) =>
        expr.combine(acc, expr.evaluate(cols))
      }
      evaluated.map(a => fn.evaluate(Array(a)))
    }
    override def combine(acc: Array[AnyRef], o: AnyRef): Array[AnyRef] = acc ++ o.asInstanceOf[Array[AnyRef]]
  }
  case class RegexPattern(pattern: String) extends Expr {
    val r = pattern.r
    override def evaluate(cols: Array[AnyRef]): AnyRef = r
  }

  val fnRegistry = mutable.HashMap.empty[String, ProcessingFunction]
  def registerPFn[T](el: (String, ProcessingFunction)) =
    el match { case (name, f) => fnRegistry.put(name, f) }
  def registerFn[T](el: (String, Array[AnyRef] => AnyRef)) = 
    el match { case (name, f) => fnRegistry.put(name, f2ProcessingFunction(name, f)) }

  implicit def f2ProcessingFunction[T](el: (String, Array[AnyRef] => AnyRef)) = {
    el match {
      case (n, f) =>
        new ProcessingFunction {
          override val name = n
          override def evaluate(args: Array[AnyRef]): AnyRef = f(args)
        }
    }
  }

  val hash = new ProcessingFunction {
    override val name = "hash"
    val hasher = MessageDigest.getInstance("MD5")

    override def evaluate(args: Array[AnyRef]): AnyRef = {
      hasher.reset()
      args.foreach { a => hasher.update(a.toString.getBytes)}
      Base64.encodeBase64URLSafeString(hasher.digest())
    }
  }
  val gsub = new ProcessingFunction {
    override val name = "gsub"
    override def evaluate(args: Array[AnyRef]): AnyRef =
      args(0).asInstanceOf[Regex].replaceAllIn(args(1).asInstanceOf[String], args(2).asInstanceOf[String])
  }
  val extract: ProcessingFunction = new ProcessingFunction {
    override val name = "extract"
    override def evaluate(args: Array[AnyRef]): AnyRef = args(0).toString match {
      case "hours" => Integer.valueOf(args(1).asInstanceOf[DateTime].hourOfDay().get())
      case "minutes" => Integer.valueOf(args(1).asInstanceOf[DateTime].minuteOfHour().get())
    }
  }

  def mathBiFn(f: (Int, Int) => Int) = (args: Array[AnyRef]) => Integer.valueOf(f(toInt(args(0)), toInt(args(1))))
  def mathUniFn(f: Int => Int) = (args: Array[AnyRef]) => Integer.valueOf(f(toInt(args(0))))

  // standard functions
  {
    registerFn(("+",       (args: Array[AnyRef]) => Integer.valueOf(toInt(args(0)) + toInt(args(1)))))
    registerFn(("toInt",   (args: Array[AnyRef]) => toInt(args(0))))
    registerFn(("toDouble",(args: Array[AnyRef]) => toDouble(args(0))))
    registerFn(("concat",  (args: Array[AnyRef]) => args(0).toString + args(1).toString))
    registerFn(("length",  (args: Array[AnyRef]) => toInt(args(0).toString.length)))
    registerFn(("find",    (args: Array[AnyRef]) => Integer.valueOf(args(0).toString.indexOf(args(1).toString))))
    registerFn(("trim",   (args: Array[AnyRef]) => args(0).toString.trim))
    registerFn(("min",     mathBiFn(math.min)))
    registerFn(("max",     mathBiFn(math.max)))
    registerFn(("abs",     mathUniFn(math.abs)))
    registerPFn(("hash",    hash))
    registerPFn(("gsub",    gsub))
    registerFn(("toDate",  (args: Array[AnyRef]) => new DateTime(args(0))))
    registerPFn(("extract", extract))
  }


  // geo functions
  {
    val gf = JTSFactoryFinder.getGeometryFactory
    registerFn("point", (args: Array[AnyRef]) => gf.createPoint(new Coordinate(toDouble(args(0)), toDouble(args(1)))))
  }

  object AwkParser extends RegexParsers {
    def lit = "[^$,()]+".r ^^ {
      case i => Lit(i)
    }
    def col    = "$" ~> "[0-9]+".r ^^ {
      case i => Col(i.toInt - 1)
    }
    def colRange = col ~ "-" ~ col ^^ {
      case s ~ "-" ~ e => ColRange(s.i, e.i)
    }
    def regexLit = "/" ~ "[^/]*".r ~ "/" ^^ {
      case "/" ~ re ~ "/" => RegexPattern(re)
    }
    def colAssignment: Parser[Expr] = col ~ "=" ~ expr ^^ {
      case i ~ "=" ~ expr => ColAssignment(i, expr)
    }
    def atom = regexLit | colRange | col | lit
    def funcName = "[^()$,]*".r
    def applyFn: Parser[Expr] = "apply(" ~ funcName ~ "," ~ rep1sep(expr, ",") ~ ")" ^^ {
      case "apply(" ~ fn ~ "," ~ args ~ ")" => Apply(fnRegistry(fn), args)
    }
    def func: Parser[Expr] = funcName ~ "(" ~ rep1sep(expr, ",") ~ ")" ^^ {
      case name ~ "(" ~ args ~ ")" => Fn(fnRegistry(name), args)
    }
    def expr   = applyFn | func | atom
    def exprs  = rep1sep(expr, ";")
    def parse(s: String): Seq[Expr] = parse(exprs, s).get
  }
  class FlowProcessor {
    outer =>

    def awk(expr: String): FlowProcessor = {
      new FlowProcessor {
        val expressions = AwkParser.parse(expr)
        println(expressions)
        override def run(args: Iterator[Array[AnyRef]]): Iterator[Array[AnyRef]] = {
          val processed = outer.run(args)
          processed.flatMap { line =>
            Try {
              expressions.foldLeft(Array.empty[AnyRef]) { (acc, expr) =>
                expr.combine(acc, expr.evaluate(line))
              }
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
        |1,  2,hello,world,2011-01-01T00:00:00.000Z
        |3,4,  hello,foo,2011-01-01T12:00:00.000Z
        |5,6,bar,   baz,2011-01-01T13:00:00.000Z
        |""".stripMargin

    val is = IOUtils.readLines(IOUtils.toInputStream(data)).iterator().map(_.asInstanceOf[String].split(",")).asInstanceOf[Iterator[Array[AnyRef]]]

    val transformed =
      new FlowProcessor()
        .awk(
          """
            |apply(trim,$1-$5)
          """.stripMargin)
        .awk(
          """
            |min(extract(minutes,toDate($5)), extract(hours,toDate($5)));
            |$1;
            |+($1, toInt($2));
            |$3;
            |$4;
            |concat($3, $4);
            |concat(myword, $4);
            |$1;
            |$2")
          """.stripMargin)
        .awk(
          """
            |hash($1-$3);
            |$1-$3;
            |$5;
            |find($4,ell);
            |gsub(/ell/, $4, ALL);
            |point($8,$9)
          """.stripMargin)

    val l = transformed.run(is).toList
    l.foreach { t => println(t.map(_.toString).mkString(",")) }
  }
}
