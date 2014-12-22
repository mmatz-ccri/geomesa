package org.locationtech.geomesa.utils.unix

import java.io._

import org.apache.commons.io.IOUtils
import org.joda.time.DateTime

import scala.collection.mutable
import scala.util.Try
import scala.util.parsing.combinator.RegexParsers

object DSL {

  trait ProcessingFunction {
    def evaluate(args: AnyRef*): AnyRef
    protected def toInt(arg: Any): Int = arg match {
      case a: Int    => a
      case a: String => a.toInt
    }
  }

  val data =
    """
      |1,2,hello,world,2011-01-01T00:00:00.000Z
      |3,4,hello,foo,2011-01-01T12:00:00.000Z
      |5,6,bar,baz,2011-01-01T13:00:00.000Z
      |""".stripMargin

  val parserConf =
    """
      |delim = ","
      |fields = [
      |  {
      |    name = "age",
      |
      |
      |  }
    """.stripMargin

  sealed trait Expr {
    def evaluate(cols: AnyRef*): AnyRef
  }
  case class Lit(l: String) extends Expr {
    override def evaluate(cols: AnyRef*): AnyRef = l
  }
  case class Col(i: Int) extends Expr {
    override def evaluate(cols: AnyRef*): AnyRef = cols(i)
  }
  case class Fn(fn: ProcessingFunction, args: Seq[Expr]) extends Expr {
    override def evaluate(cols: AnyRef*): AnyRef = {
      fn.evaluate(args.map(_.evaluate(cols: _*)): _*)
    }
  }

  val fnRegistry = mutable.HashMap.empty[String, ProcessingFunction]
  def registerFn[T <: ProcessingFunction](name: String, f: T) = fnRegistry.put(name, f)

  implicit def f2ProcessingFunction(f: (AnyRef*) => AnyRef): ProcessingFunction =
    new ProcessingFunction {
      override def evaluate(args: AnyRef*): AnyRef = f(args)
    }

  registerFn("+",       (args: AnyRef*) => Integer.valueOf(args(0).asInstanceOf[Int] + args(1).asInstanceOf[Int]))
  registerFn("toInt",   (args: AnyRef*) => Integer.valueOf(args(0).asInstanceOf[String]))
  registerFn("concat",  (args: AnyRef*) => args(0).toString + args(1).toString)
  registerFn("gsub",    (args: AnyRef*) => {
    val in = args(0).toString
    val
  }
  )
  registerFn("min",     (args: AnyRef*) => Integer.valueOf(math.min(args(0).asInstanceOf[Int], args(1).asInstanceOf[Int])))
  registerFn("toDate",  (args: AnyRef*) => new DateTime(args(0)))
  registerFn("extract", (args: AnyRef*) => args(0).toString match {
    case "hours" => Integer.valueOf(args(1).asInstanceOf[DateTime].hourOfDay().get())
    case "minutes"  => Integer.valueOf(args(1).asInstanceOf[DateTime].minuteOfHour().get())
  })


  object AwkParser extends RegexParsers {
    def lit = "[^$,()]+".r ^^ {
      case i => Lit(i)
    }
    def col    = "$" ~> "[0-9]+".r ^^ {
      case i => Col(i.toInt - 1)
    }
    def atom = col | lit
    def func: Parser[Expr] = "[^($,]*".r ~ "(" ~ rep1sep(expr, ",") ~ ")" ^^ {
      case name ~ "(" ~ args ~ ")" => Fn(fnRegistry(name), args)
    }
    def expr   = func | atom
    def exprs  = rep1sep(expr, ";")
    def parse(s: String): Seq[Expr] = parse(exprs, s).get
  }
  implicit class RichReader(val is: BufferedReader) extends AnyVal {

    def awk(expr: String): BufferedReader = {
      val expressions = AwkParser.parse(expr)
      val reader = new Reader() {
        var curLine: String = null
        var srcPos = 0

        override def close(): Unit = is.close()

        override def read(cbuf: Array[Char], off: Int, len: Int): Int = {
          if (curLine == null) curLine = readLine()
          if (curLine != null) {
            val toWrite =
              if (len > curLine.length - srcPos) curLine.length - srcPos
              else len
            System.arraycopy(curLine.toCharArray, srcPos, cbuf, off, toWrite)
            srcPos = srcPos + toWrite
            if (srcPos >= curLine.length) {
              srcPos = 0
              curLine = null
            }
            toWrite
          } else -1
        }

        def readLine(): String = {
          val line = is.readLine()
          if(line == null) null
          else Try {
            val cols = line.split(",")
            expressions.map(_.evaluate(cols: _*)).mkString(",")
          }.getOrElse(line)
        }
      }
      new BufferedReader(reader)
    }
  }

  import scala.collection.JavaConversions._
  def main(args: Array[String]) {
    val is = new BufferedReader(new InputStreamReader(IOUtils.toInputStream(data)))
    val transformed =
      is.awk("min(extract(minutes,toDate($5)),extract(hours,toDate($5)));$1;+($1,toInt($2));$3;$4;concat($3,$4);concat(myword,$4)")
        .awk("$5")

    val lines = IOUtils.readLines(transformed).asInstanceOf[java.util.List[String]]
    println(lines.mkString("\n"))
  }
}
