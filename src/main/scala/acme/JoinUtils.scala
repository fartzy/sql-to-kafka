package acme

import scala.language.implicitConversions
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.ByteArrayOutputStream
import scala.collection.mutable
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Map
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.streams.kstream.Predicate
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}


class DateBetweenPredicate(val compareField: String,
                           val fromField: String,
                           val toField: String) extends Predicate[String, JsonNode] {

    override def test(key: String, value: JsonNode): Boolean = {
        val d = compareField match {
            case "CurrentDate" => LocalDate.now
            case "CurrentDate+1y" => LocalDate.now.plusYears(1)
            case "CurrentDate-1y" => LocalDate.now.minusYears(1)
            case _ => compareField
        }
        val comp = JoinUtils.normalizeDate(d.toString).getOrElse("")
        val startDate = JoinUtils.normalizeDate(fromField).getOrElse("")
        val endDate = JoinUtils.normalizeDate(toField).getOrElse("")
        val rc = comp >= startDate && comp <= endDate
        rc
    }
}

class InStringPredicate(val field: String, val values: List[String]) extends Predicate[String, JsonNode] {
    override def test(key: String, value: JsonNode): Boolean = {
        val s = value.get(field)
        values.contains(s)
    }
}

class InLongPredicate(val field: String, val values: List[Long]) extends Predicate[String, JsonNode] {
    override def test(key: String, value: JsonNode): Boolean = {
        val s = value.get(field)
        values.contains(s.asLong)
    }
}

class InDoublePredicate(val field: String,
                        val values: List[Double]) extends Predicate[String, JsonNode] {
    override def test(key: String, value: JsonNode): Boolean = {
        val s = value.get(field)
        values.contains(s.asDouble)
    }
}

class AndPredicate(val filters: List[Predicate[String, JsonNode]]) extends Predicate[String, JsonNode] {
    override def test(key: String, value: JsonNode) = {
        var rc = true
        breakable {
            for (f <- filters) {
                if (f.test(key, value) == false) {
                    rc = false
                    break
                }
            }
        }
        rc
    }
}

class OrPredicate(val filters: List[Predicate[String, JsonNode]]) extends Predicate[String, JsonNode] {
    override def test(key: String, value: JsonNode) = {
        var rc = false
        breakable {
            for (f <- filters) {
                if (f.test(key, value) == true) {
                    rc = true
                    break
                }
            }
        }
        rc
    }
}

object JoinUtils {

    val dateFormats = List("MMM dd uuuu hh:mm:ss:SSSa",
                           "MMMM dd uuuu hh:mm:ss:SSSa",
                           "MMM  d uuuu hh:mm:ss:SSSa",
                           "MMMM  d uuuu hh:mm:ss:SSSa",
                           "dd/MM/uuuu",
                           "MMM dd, uuuu",
                           "MMM d, uuuu",
                           "MMMM dd, uuuu",
                           "MMMM d, uuuu",
                           "dd MMMM uuuu",
                           "d MMMM uuuu",
                           "dd MMM uuuu",
                           "d MMM uuuu",
                           "dd-MM-uuuu",
                           "d-MM-uuuu",
                           "uuuu-MM-dd hh:mm:ss",
                           "yyyy-MM-dd HH:mm:ss")
        .map(p => (p, DateTimeFormatter.ofPattern(p)))

    val iso8601DateFormatter:DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

    def normalizeDate(dateStr: String): Option[String] = {
        val trimmedDate = dateStr.trim
        if(trimmedDate.isEmpty)
            None
        else {
            dateFormats.toStream.map {
                case (pattern, fmt) =>
                    Try(fmt.parse(trimmedDate))
            }.find(_.isSuccess).map{ t =>
                iso8601DateFormatter.format(t.get) }
        }
    }

    def reKey(lv: JsonNode, lst: List[String], out: mutable.ListBuffer[String]): String = lst match {
        case Nil => ""
        case h :: t => {
            val payload = lv.get("payload")
            val s = payload.get(h)
            out.append(s.asText)
            out.toString() + reKey(lv, t, out)
        }
    }

    //def joinValues(lv: JsonNode, rv: JsonNode, gen: JsonGenerator, lst: List[String]): Unit = lst match {
    def joinValues(lv: JsonNode, rv: JsonNode, node: ObjectNode, lst: List[String]): Unit = lst match {
        case Nil =>
        case h :: t => {
            val payload = lv.get("payload")
            val value = payload.get(h) match {
                case v: JsonNode => v.asText
                case null => {
                    val payload = rv.get("payload")
                    val  node = payload.get(h)
                    if(node == null) "" else node.asText()
                }
            }
            node.put(h, value)
            joinValues(lv, rv, node, t)
        }
    }

    def convertByteArrayToJsonObject(ba: Array[Byte]):  JsonNode = {
        val objectMapper = new ObjectMapper()
        val node = objectMapper.readValue(ba, classOf[JsonNode])
        node
    }

    def convertJsonToByteArray(obj: Map[String, String]): Array[Byte] = {
        var ba: Array[Byte] = null
        val out = new ByteArrayOutputStream()
        try {
            val mapper = new ObjectMapper()
            mapper.writeValue(out, obj)
            ba = out.toByteArray()
        } catch  {
            case e: Exception => {
                println("In convertJsonToByteArray => can't convert record to byte array:::" + e.fillInStackTrace())
            }
        } finally {
            out.close()
        }
        ba
    }

    def removeEntryFromJsonNode(fields: List[String], bytes: Array[Byte]): Array[Byte] = {
        val objectMapper = new ObjectMapper
        val rootNode = objectMapper.readTree(bytes)
            if (rootNode.isInstanceOf[ObjectNode]) {
                for(field <- fields) {
                    val payload = rootNode.get("payload")
                    if (payload.has(field)) {
                        val obj: ObjectNode  = payload.asInstanceOf[ObjectNode]
                        obj.remove(field)
                    }
                }
            }
        val out = new ByteArrayOutputStream()
        objectMapper.writeValue(out, rootNode)
        val ba: Array[Byte] = out.toByteArray()
        ba
    }

    def validateFilterExpression(lst: List[String]): Boolean = {
        var rc: Boolean = true

        if (lst.size != 4) {
            rc = false
        } else {
            val cmd = lst(0).split("=").map(_.trim)
            val arg = lst(1).split("=").map(_.trim)
            val begin = lst(2).split("=").map(_.trim)
            val end = lst(3).split("=").map(_.trim)

            if (cmd(1).toLowerCase != "between_date" ||
                arg(0).toLowerCase.trim != "arg" ||
                begin(0).toLowerCase.trim != "begin" ||
                end(0).toLowerCase.trim != "end") {
                rc = false
            } else {
                val a = arg(1).split("""\.""")
                val b = begin(1).split("""\.""")
                val e = end(1).split("""\.""")

                if ((a(0).toLowerCase.trim != "left" && a(0).toLowerCase.trim != "right") ||
                    (b(0).toLowerCase.trim != "left" && b(0).toLowerCase.trim != "right") ||
                    (e(0).toLowerCase.trim != "left" && e(0).toLowerCase.trim != "right")) {
                    rc = false
                }
            }
        }
        rc
    }

    def buildPredicate(filterExpression: List[String], left: JsonNode, right: JsonNode): Predicate[String, JsonNode] = {
        val arg = filterExpression(1).split("=").map(_.trim)
        val begin = filterExpression(2).split("=").map(_.trim)
        val end = filterExpression(3).split("=").map(_.trim)

        val argExpr = arg(1).split("""\.""")
        val beginExpr = begin(1).split("""\.""")
        val endExpr = end(1).split("""\.""")

        val compareField = if(argExpr(0).toLowerCase.trim == """left""") {
            val field = argExpr(1).trim
            left.get(field).asText
        } else {
            val field = argExpr(1).trim
            right.get(field).asText
        }

        val beginField = if(beginExpr(0).toLowerCase.trim == """left""") {
            val field = beginExpr(1).trim
            left.get(field).asText
        } else {
            val field = beginExpr(1).trim
            right.get(field).asText
        }

        val endField = if(endExpr(0).toLowerCase.trim == """left""") {
            val field = endExpr(1).trim
            left.get(field).asText
        } else {
            val field = endExpr(1).trim
            right.get(field).asText
        }

        val datePred = new DateBetweenPredicate(compareField, beginField, endField)
        datePred
    }

    def buildPostJoinPredicate(filterExpression: List[String], value: JsonNode): Predicate[String, JsonNode] = {
        val arg = filterExpression(1).split("=").map(_.trim)
        val begin = filterExpression(2).split("=").map(_.trim)
        val end = filterExpression(3).split("=").map(_.trim)

        val argExpr = arg(1).split("""\.""")
        val beginExpr = begin(1).split("""\.""")
        val endExpr = end(1).split("""\.""")

        val payload = value.get("payload")

        val field = argExpr(1).trim
        val compareField = if(field != null) payload.get(field).asText else ""

        val field2 = beginExpr(1).trim
        val beginField = if(field2 != null) payload.get(field2).asText else ""

        val field3 = endExpr(1).trim
        val endField = if(field3 != null) payload.get(field3).asText else ""

        val datePred = new DateBetweenPredicate(compareField, beginField, endField)
        datePred
    }
}
