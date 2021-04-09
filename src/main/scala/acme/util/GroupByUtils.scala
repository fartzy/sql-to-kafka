package acme

import scala.collection.mutable
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import org.apache.kafka.streams.kstream.Predicate
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks.{break, breakable}
import org.apache.kafka.streams.kstream._

import _root_.scala.language.implicitConversions
import _root_.scala.collection.JavaConverters._
import _root_.scala.reflect.ClassTag
import annotation.tailrec
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.ConfigFactory
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.UUID

import acme.config.GroupByConfig



//case class SumReducer[V](v1: V) extends Reducer[V]{
//  override
//  def apply(v1: V, v2: V) : V = {
//    (v1.toString.toLong + v2.toString.toLong).toString.asInstanceOf[V]
//  }
//}
//
//case class TrivialReducer[V](v1: V) extends Reducer[V]{
//  override
//  def apply(v1: V, v2: V) : V = {
//    v2
//  }
//}


class AggContext(groupByConfig: GroupByConfig) {

  /* Types of Aggregations */
  final val ADDER_TYPE = "Adder"
  final val SUBTRACTOR_TYPE = "Subtractor"
  final val VALUE_MAPPER_TYPE = "Value Mapper"

  /* Configurations from Manifest file */
  val getListGroupByColName = groupByConfig.groupByColumnList
  lazy val getListAggregatorJNode: List[Aggregation[_]] = createAggColumnList()
  lazy val getListAggregator: List[Aggregation[_]] = getListAggregatorJNode
  lazy val getListValToAgg: List[String] = getListAggregator.map(_.aggColumn)
  val getAliases: List[String] = groupByConfig.aliasList
  val dateFormatList = groupByConfig.dateFormatList

  def createAggColumnList(): List[Aggregation[_]] = {

    @tailrec
    def iterateThroughLists(l0: List[String],l1: List[String], l2: List[String], l3: List[Aggregation[_]]):List[Aggregation[_]] = (l0,l1,l2) match {
      case(_,Nil, Nil) => l3
      case(hl0::tl0, hl1::tl1, hl2::tl2) => iterateThroughLists(tl0, tl1, tl2, l3 :+ Aggregation(hl2.trim, hl1.trim, hl0.trim))
      case _ =>  throw new Exception(AggContext.getExceptionMessage("config"))
    } //end iterateThroughLists

    iterateThroughLists(getAliases
      , groupByConfig.aggregateColumnList
      , groupByConfig.aggregateOperationList
      , List.empty[Aggregation[_]]) :+ Aggregation("json", "Key=" + getListGroupByColName.toString, "Key=" + getListGroupByColName.toString )

  } //end createAggColumnList



  /*
   * Serializaion methods
   * Data is serialized two times.  First from the value to a byte array which forms a List of byte arrays.
   * Then from the list of bytearray to a large byte array.
   * 1. `serializeFromListAggToListByteArray` is called to serialize the value to a Byte Array and return a List of ByteArray
   * 2. `serializeListByteArraytoByteArray` is called to serialize the List of Byte Array to a ByteArray
   */
  def serializeFromListAggToListByteArray(aggVals: List[_], aggColsIn: List[Aggregation[_]] = this.getListAggregator) = {

    @tailrec
    def iterateListAggToListBA(aggVals: List[_]
                               , aggColsIn: List[Aggregation[_]] = this.getListAggregator
                               , acc: List[Array[Byte]]): List[Array[Byte]] = (aggVals, aggColsIn) match {
      case (Nil, Nil)  => if (acc.length > 0) acc else throw new RuntimeException(AggContext2.getExceptionMessage("empty-config"))
      case (hV :: tV, hC :: tC) => iterateListAggToListBA(tV, tC, acc :+ hC.serialize(hV))
      case _ => throw new RuntimeException("AggVals = " + aggVals.map(v => v.toString + "; ") +
        " - AggColsIn = " + aggColsIn.map(v => v.aggType + "; ") + " " + AggContext2.getExceptionMessage("config"))
    } //end serializeFromListAggToListByteArray

    iterateListAggToListBA(aggVals, aggColsIn, List.empty[Array[Byte]])
  } //end serializeFromListAggToListByteArray

  def serializeFromListByteArraytoByteArray(value: List[Array[Byte]]): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  } //end serializeListByteArraytoByteArray




  /*
   * Deserializaion methods
   * Data is also deserialized two times.  First from the value to a byte array to a List of Byte Array then from the list of bytearray to a List of values.
   * 1. `deserializeToListByteArrayFromByteArray` is called to serialize the value to a List of Byte Array from a large ByteArray
   * 2. `deserializeToListAggFromListByteArray` is called to serialize the List of Byte Array to a List of Values that can be operated on
   */

  def deserializeToListByteArrayFromByteArray(bytes: Array[Byte]): List[Array[Byte]] = {

    //The EOF Exception is not clear that is just needs to be deserialized.  Leaving it serialized actually allows for easier debugging
    //by leaving it clear that it is a serialization problem
    val list = {
      if (bytes.length > 0) {
        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val value = ois.readObject
        ois.close()
        value.asInstanceOf[List[Array[Byte]]]
      } else List(Array(0.toByte))
    }
    list
  } //end deserializeToListByteArrayFromByteArray

  def deserializeFromListByteArrayToListAgg(isAgg: Boolean, aggVals: List[Array[Byte]], aggCols: List[Aggregation[_]] = this.getListAggregator) = {

    @tailrec
    def iterateListByteArray(isAgg: Boolean
                             , aggVals: List[Array[Byte]]
                             , aggCols: List[Aggregation[_]]
                             , acc: List[_]): List[_] = (aggVals, aggCols) match {
      case (Nil, Nil) => acc
      case (hV::tV, hC::tC) if isAgg => iterateListByteArray(isAgg, tV, tC, acc :+ hC.deserializeAgg(hV))
      case (hV::tV, hC::tC) => iterateListByteArray(isAgg, tV, tC, acc :+ hC.deserializeVal(hV))
      case _ => throw new RuntimeException(AggContext2.getExceptionMessage("config"))
    } //end  iterateListByteArray

    iterateListByteArray(isAgg, aggVals, aggCols, List.empty[Any])
  } //end deserializeToListAggFromListByteArray


  /*
   * Initialization methods
   * Aggregation Initialization needs to happen in the beginning of the aggregation, because there needs to be a initial value.
   * 1. `getListAggInitVals` is called to serialize the value to a List of Byte Array from a large ByteArray
   */

  def initialValues(aggColsIn: List[Aggregation[_]] = this.getListAggregator) : List[_] = {

    @tailrec
    def iterateAggInitVals(aggColsIn: List[Aggregation[_]] = this.getListAggregator, acc: List[_] = List.empty[Any]): List[_] = aggColsIn match {
      case Nil if (acc.length > 0) => acc
      case Nil => throw new RuntimeException(AggContext.getExceptionMessage("empty-config"))
      case hC::tC => iterateAggInitVals(tC, acc:+hC.initializationValue)
    } //end iterateAggInitVals

    iterateAggInitVals(aggColsIn,List.empty[Any])

  } //end getListAggInitVals



  /*
   * Mapping and Grouping methods
   * First there are the fields that need to be picked from the kafka message.  Then the new key needs to be picked.
   * 1. `getListAggInitVals` is called to serialize the value to a List of Byte Array from a large ByteArray
   * 2. `groupByKey` is called to choose a new String that will be a concatenation of the string representation of the group by fields
   */


  //returns a tuple with the orignal JsonNode as the first element and the second element as a list of strings where each string is
  //the spcific value of the specific column of the specific record
  def getListValsToAgg(ba: Array[Byte]):  (JsonNode, List[String]) = {

    val lv = AggContext2.convertByteArrayToJsonObject(ba)

    @tailrec
    def getColumnValues(lv: JsonNode,lst: List[String] = getListValToAgg, acc: List[String]): List[String] = lst match {
      case Nil => acc
      case hV::tV => getColumnValues(lv,tV, acc :+ lv.get("payload").get("payload").findValue(hV).asText)
    } //end getColumnValues


    (lv, getColumnValues(lv, getListValToAgg.init, List.empty[String]):+ lv.toString)
  } //end getListValsToAgg


  def groupByKey(ba: Array[Byte]): (JsonNode, String) = {

    val lv = AggContext2.convertByteArrayToJsonObject(ba)

    @tailrec
    def reKey(lv: JsonNode, lst: List[String], acc: List[String]): List[String] = lst match {
      case Nil => acc
      case hV::tV => reKey(lv, tV, acc :+ lv.get("payload").get("payload").findValue(hV).asText )
    } //end reKey

    (lv,reKey(lv, getListGroupByColName, List.empty[String])
      .reduce(_ + "-" + _))

  } //end groupByKey





  def returnJsonNode(values:List[String]): JsonNode = {
    //val jn: JsonNode = getListAggregator.head.jNode.get
    val mapper: ObjectMapper = new ObjectMapper()
    val jn: JsonNode = mapper.readTree(values.last)

    val outerPayload = jn.deepCopy[ObjectNode]

    val parentUUID = jn.get("payload")
      .deepCopy[ObjectNode]
      .get("UUID")
      .asText()

    val intermediatePayload: ObjectNode = jn
      .get("payload")
      .deepCopy[ObjectNode]
      .put("UUID",UUID.randomUUID().toString)
      .put("parentUUID",parentUUID)

    val intstant = Instant.now.toString
    val innerPayloadNoAgg: ObjectNode = mapper.createObjectNode
      .put("createdAt", intstant)

    @tailrec
    def getGroupByValues(lColNames: List[String] = getListGroupByColName
                         , lVals: List[(String, String)] = List.empty[(String, String)]): List[(String, String)] = lColNames match {
      case Nil => lVals
      case hc::tcs => getGroupByValues(tcs, lVals:+(hc,jn.get("payload").get("payload").findValue(hc).asText))

    } //end reKey

    @tailrec
    def buildJsonNode(lst: List[String],
                      lA: List[Aggregation[_]],
                      lGlst: List[(String, String)],
                      acc: ObjectNode): ObjectNode = (lst, lGlst, lA) match {
      case (Nil, Nil, Nil) => acc
      case (Nil, lH::lT, Nil) => buildJsonNode(Nil, Nil, lT, acc.put(lH._1, lH._2))
      case (hV::tV, lGlst, hA::tA) => buildJsonNode(tV, tA, lGlst, acc.put(hA.alias, hV))
      case _ => mapper.createObjectNode
    } //end reKey

    val returnNode:JsonNode = outerPayload.set("payload"
      , intermediatePayload.set("payload"
        , buildJsonNode(values.init, getListAggregator.init, getGroupByValues(), innerPayloadNoAgg)))


    returnNode


  } //end getJsonNode
  /*
  * Aggregation methods
  * Aggregation needs to happen in three places.  The adder, the subtractor, and a valueMapper at the end of the processing
  * 1. `aggregate` will do the majority of the processing, and there are three variants (curried functions) of aggregate
  * 2. `aggregateAdder` will be used to aggregate the adder, which is the main aggregation
  * 3. `aggregateSubtractor` will be used to aggregate the subtractor, which removes the old value from the aggregation
  * 4. `valueMapper` will be used to do calculations that need additional processing like median or mean
  */

  def aggregate(aggType: String)
               (key: String,
                value: Array[Byte],
                agg: Array[Byte]): Array[Byte] = {
    //println("processType: " + processType + "-- key: " + key + "-- value:" + deserializeToListByteArrayFromByteArray(value).toString )
    val  valuesList = this.deserializeToListByteArrayFromByteArray(value)
    val  aggsList = this.deserializeToListByteArrayFromByteArray(agg)

    @tailrec
    def processAggregate(valList: List[Array[Byte]]
                         , aggList: List[Array[Byte]]
                         , aggColList: List[Aggregation[_]]
                         , acc: List[_]): List[_] = (valList, aggList, aggColList)  match {
      case (Nil, Nil, Nil) => acc
      case (hV::tV, hAV::tAV, hC::tC) =>
        if (aggType == ADDER_TYPE) processAggregate(tV, tAV, tC, acc :+ hC.adder(hV, hAV))
        else if (aggType == SUBTRACTOR_TYPE) processAggregate(tV, tAV, tC, acc :+ hC.subtractor(hV, hAV))
        else processAggregate(tV, tV/* tV is redundant on purpose because there is no aggregate - already aggregated */, tC, acc :+ hC.valueMapper(hV))
      case _ => throw new Exception("Exception: " + s"${aggType}" + AggContext2.getExceptionMessage("config"))
    } //end processAggregate

    this.serializeFromListByteArraytoByteArray(
      this.serializeFromListAggToListByteArray(
        processAggregate(valuesList, aggsList, this.getListAggregator, List.empty[Any]), this.getListAggregator)
    )
  } //end aggregate

  /* curried methods from aggregate */
  def aggregateAdder = aggregate(ADDER_TYPE)(_,_,_)
  def aggregateSubtractor = aggregate(SUBTRACTOR_TYPE)(_,_,_)
  def valueMapper = aggregate(VALUE_MAPPER_TYPE)("Dummy Key",_:Array[Byte],Array.empty[Byte])
}


object AggContext {
  /* used to crete the singleton instance.  There is only one AgContext ever per application */
  private var _instance : Option[AggContext2] = None

  def instance(groupByConfig: GroupByConfig): AggContext2 = {
    if (_instance == None)
      _instance = Option[AggContext2](new AggContext2(groupByConfig))
    _instance.get
  } //end instance

  def getDateFormatList = {
    _instance.get.dateFormatList
  } //end getDateFormatList

  def convertJsonToByteArray(obj: Map[String, String]): Either[String, Array[Byte]] = {
    var ba: Array[Byte] = Array.empty[Byte]
    val out = new ByteArrayOutputStream()
    try {
      val mapper = new ObjectMapper()
      mapper.writeValue(out, obj)
      ba = out.toByteArray()
    }
    catch {
      case e: Exception => {
        Left("convertJsonToByteArray => can't convert record to byte array: " + e.fillInStackTrace())
      }
    }
    finally {
      out.close()
    }
    Right(ba)
  } //end convertJsonToByteArray

  def convertByteArrayToJsonObject(ba: Array[Byte]):  JsonNode = {
    val objectMapper = new ObjectMapper()
    val node = objectMapper.readValue(ba, classOf[JsonNode])
    node
  } //end convertByteArrayToJsonObject

  /* utility method to return argument based exception messages */
  def getExceptionMessage(str: String) = str match {
    case "config" => "Column values and column aggregation lists do not have same ordinal positions - perhaps the lengths are different"
    case "not-serialized" => "Passed an empty byte array into deserialization method. AN EOF Exception will be thrown"
    case "empty-config" => "there exists an empty list of config values"
    case "date-parse" => "No acceptable date parsing"
    case "unknown-config" => "Unknown Config"
    case _ => "Program failed"
  } //end getExceptionMessage



  def normalizeDate(format:String, dateStr: String): Option[String] = {

    val dateFormats = (format :: List("MMM dd uuuu hh:mm:ss:SSSa",
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
      "yyyy-MM-dd HH:mm:ss"))
      .map(p => (p, DateTimeFormatter.ofPattern(p)))

    val trimmedDate = dateStr.trim
    if (trimmedDate.isEmpty)
      None
    else {
      val t = dateFormats.map {
        case (patternString, pattern) =>
          Try(LocalDateTime.parse(dateStr, pattern))
      }.find(_.isSuccess)

      t match {
        case Some(Success(g)) => Some(g.toEpochSecond(ZoneOffset.UTC).toString)
        case Some(Failure(g)) => None
        case None => None
      } //end match
    } //end else
  } //end normalizeDate
} //end object AggContext



/*
 * Aggregation Abstract Class and SubClasses
 */
object Aggregation {
  def apply[T](aggType:String, aggColumn: String, alias:String) = aggType match {
    case a if a matches "(?i)sum" => new SumAggregation(aggColumn,alias)
    case a if a matches "(?i)max" => new MaxAggregation(aggColumn,alias)
    case a if a matches "(?i)min" => new MinAggregation(aggColumn,alias)
    case a if a matches "(?i)concat" => new ConcatAggregation(aggColumn,alias)
    case a if a matches "(?i)avg" => new AvgAggregation(aggColumn,alias)
    case a if a matches "(?i)json" => new JsonAggregation(aggColumn,alias)
    case a if a matches "(?i)var" => new VarianceAggregation(aggColumn,alias)
    case a => throw new Exception(a + " - " + AggContext2.getExceptionMessage("unknown-config"))
  } //end apply
} //end Aggregation

abstract class Aggregation[T](val aggType: String, val aggColumn: String, val alias: String)  {
  type InitType = T

  def serialize(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  def deserialize(isVal: Boolean)(bytes: Array[Byte])(cbf: String => InitType ):  InitType = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    if (isVal) cbf(value.asInstanceOf[String]) else value.asInstanceOf[InitType]
  }

  def deserializeVal(bytes: Array[Byte]) = deserialize(true)(bytes: Array[Byte])(convertFn)
  def deserializeAgg(bytes: Array[Byte]) = deserialize(false)(bytes: Array[Byte])(convertFn)
  def valueMapper(value: Array[Byte]): InitType = deserializeAgg(value)


  //abstract members that need to be implemented
  def initializationValue: InitType
  def convertFn: String => InitType
  def adder(value: Array[Byte], agg: Array[Byte]) : InitType
  def subtractor(oldValue: Array[Byte], agg: Array[Byte]) : InitType = deserializeAgg(agg)
}

abstract class DoubleAggregation(aggType: String, aggColumn: String, alias:String) extends Aggregation[Double](aggType, aggColumn, alias) {
  override type InitType = Double
  def convertFn: String => InitType = _.toLong.toDouble
}


abstract class StringAggregation(aggType: String, aggColumn: String, alias: String) extends Aggregation[String](aggType, aggColumn, alias) {
  override type InitType = String
  def convertFn: String => InitType = v => v
}

class SumAggregation(aggColumn: String, alias: String) extends DoubleAggregation(aggType = "sum", aggColumn, alias) {
  def initializationValue: InitType = 0.toDouble
  def adder(value: Array[Byte], agg: Array[Byte]): InitType = deserializeVal(value) + deserializeAgg(agg)
  override def subtractor(oldValue: Array[Byte], agg: Array[Byte]) : InitType = deserializeAgg(agg) - deserializeVal(oldValue)
}

class MaxAggregation(aggColumn: String, alias: String) extends StringAggregation(aggType = "max", aggColumn, alias)  {

  var dateFlag: Boolean = false
  val formatString = AggContext2.getDateFormatList.headOption.getOrElse("yyyy-MM-dd HH:mm:ss")
  val dft = DateTimeFormatter.ofPattern(formatString)
  def changetoSecondsFromEpoch(value: Array[Byte]): Option[String] = AggContext2.normalizeDate(formatString, deserializeVal(value))
  def initializationValue: InitType = /*if valid date then convert to seconds from Epoch and set isDate flag*/ Long.MinValue.toString
  def adder(value: Array[Byte], agg: Array[Byte]): InitType = {
    changetoSecondsFromEpoch(value) match {
      case None => dateFlag = false; if (deserializeVal(value).toLong >= deserializeAgg(agg).toLong) deserializeVal(value) else deserializeAgg(agg)
      case dateVal => dateFlag = true; if (dateVal.get.toLong >= deserializeAgg(agg).toLong) dateVal.get else deserializeAgg(agg)
    }
  }

  override def valueMapper(value: Array[Byte]): InitType = {
    val deserialized = deserializeAgg(value).toLong

    if (dateFlag) {
      val formattedDateString = dft.format(LocalDateTime.ofEpochSecond(deserialized, 0, ZoneOffset.UTC))
      formattedDateString
    } else {
      deserialized.toString
    }
  }

}

class MinAggregation(aggColumn: String, alias: String) extends StringAggregation(aggType = "min", aggColumn, alias) {

  var dateFlag: Boolean = false
  val formatString = AggContext2.getDateFormatList.headOption.getOrElse("yyyy-MM-dd HH:mm:ss")
  val dft = DateTimeFormatter.ofPattern(formatString)
  def changetoSecondsFromEpoch(value: Array[Byte]): Option[String] = AggContext2.normalizeDate(formatString, deserializeVal(value))
  def initializationValue: InitType = /*if valid date then convert to seconds from Epoch and set isDate flag*/ Long.MaxValue.toString
  def adder(value: Array[Byte], agg: Array[Byte]): InitType = {
    changetoSecondsFromEpoch(value) match {
      case None => dateFlag = false; if (deserializeVal(value).toLong < deserializeAgg(agg).toLong) deserializeVal(value) else deserializeAgg(agg)
      case dateVal => dateFlag = true; if (dateVal.get.toLong < deserializeAgg(agg).toLong) dateVal.get else deserializeAgg(agg)
    }
  }

  override def valueMapper(value: Array[Byte]): InitType = {
    val deserialized = deserializeAgg(value).toLong

    if (dateFlag) {
      val formattedDateString = dft.format(LocalDateTime.ofEpochSecond(deserialized, 0, ZoneOffset.UTC))
      formattedDateString
    } else {
      deserialized.toString
    }
  }
}

class ConcatAggregation(aggColumn: String, alias: String) extends StringAggregation("concat", aggColumn, alias) {
  def initializationValue: InitType = ""
  def adder(value: Array[Byte], agg: Array[Byte]): InitType = if (deserializeAgg(agg).length > 0) {
    deserializeAgg(agg) + "||" + deserializeVal(value)
  } else {
    deserializeAgg(agg) + deserializeVal(value)
  }

}

class AvgAggregation(aggColumn: String, alias: String)extends StringAggregation("avg", aggColumn, alias) {
  def initializationValue: InitType = "0-0"
  def adder(newValue: Array[Byte], agg: Array[Byte]): InitType = {
    val newSum = deserializeAgg(agg).split('-').head.toDouble + deserializeVal(newValue).toDouble
    val newCount = deserializeAgg(agg).split('-').last.toDouble + 1L
    newSum.toString + "-" + newCount.toString
  }
  override def subtractor(oldValue: Array[Byte], agg: Array[Byte]): InitType = {
    val newSum = deserializeAgg(agg).split('-').head.toDouble - deserializeVal(oldValue).toDouble
    val newCount = deserializeAgg(agg).split('-').last.toDouble
    newSum.toString + "-" + newCount.toString
  }
  override def valueMapper(value: Array[Byte]): InitType = {
    val sum = deserializeAgg(value).split('-').head.toDouble
    val counter = deserializeAgg(value).split('-').last.toDouble
    val average = sum / counter
    average.toString
  }
}

class JsonAggregation(aggColumn: String, alias: String) extends StringAggregation(aggType = "json", aggColumn, alias) {
  def initializationValue: InitType = ""
  def adder(value: Array[Byte], agg: Array[Byte]): InitType = deserializeVal(value)
  override def subtractor(oldValue: Array[Byte], agg: Array[Byte]) : InitType = deserializeVal(oldValue)
}

class VarianceAggregation(aggColumn: String, alias: String) extends StringAggregation("var", aggColumn, alias) {

  //TODO -- Add an initializer that can be broken into two halves and the second half has a second splitter. The second half will be the list
  //of values to be iterated again to find the variance
  def initializationValue: InitType = "0-0--0"
  def adder(newValue: Array[Byte], agg: Array[Byte]): InitType = {
    //TODO -- The adder will do three things - increment the count and the sum to find the mean later on and then append another value to the
    //list of values
    deserializeAgg(agg)
  }

  override def subtractor(oldValue: Array[Byte], agg: Array[Byte]): InitType = deserializeAgg(agg)

  override def valueMapper(value: Array[Byte]): InitType = {
    //TODO -- The valueMapper will do two things - computer the mean and then subtract the mean from the value of each of the values of the list
    // then the values will be squared and the swquare root of that will be taken.  Then that square root will be divided by N - 1 where N
    // is the increment
    deserializeAgg(value)
  }
}
