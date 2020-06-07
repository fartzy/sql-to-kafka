import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import acme.{AggContext2, Aggregation2}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
def serialize(value: Any): Array[Byte] = {
  val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
  val oos = new ObjectOutputStream(stream)
  oos.writeObject(value)
  oos.close()
  stream.toByteArray
}

def deserializeString(bytes: Array[Byte]):  String = {
  val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
  val value = ois.readObject
  ois.close()
  //if (isVal) cbf(value.asInstanceOf[String]) else value.asInstanceOf[String]
  value.asInstanceOf[String]
}
def deserializeDouble(bytes: Array[Byte]):  Double = {
  val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
  val value = ois.readObject
  ois.close()
  //if (isVal) cbf(value.asInstanceOf[String]) else value.asInstanceOf[Double]
  value.asInstanceOf[Double]
}


import scala.annotation.tailrec

val jsonString:String = """{"k1":"v1","k2":"v2","k3":"7.0"}""";
val mapper: ObjectMapper = new ObjectMapper
val actualObj: JsonNode = mapper.readTree(jsonString);
def createColumnList() = {
  List(Aggregation2("concat", "out1","k2"),
       Aggregation2("sum","out2","k3"))
}
lazy val getListAggregator1: List[Aggregation2[_]] = createColumnList()
def serializeFromListAggToListByteArray(aggVals: List[_], aggColsIn: List[Aggregation2[_]] = getListAggregator1) = {

  @tailrec
  def iterateListAggToListBA(aggVals: List[_]
                             , aggColsIn: List[Aggregation2[_]] = getListAggregator1
                             , acc: List[Array[Byte]]): List[Array[Byte]] = (aggVals, aggColsIn) match {
    case (Nil, Nil)  => if (acc.length > 0) acc else throw new RuntimeException(AggContext2.getExceptionMessage("empty-config"))
    case (hV :: tV, hC :: tC) => iterateListAggToListBA(tV, tC, acc :+ hC.serialize(hV))
  } //end serializeFromListAggToListByteArray

  iterateListAggToListBA(aggVals, aggColsIn, List.empty[Array[Byte]])
} //end serializeFromListAggToListByteArray

def s2(aggVals: List[_], aggColsIn: List[Aggregation2[_]] = getListAggregator1) = {

  List(serialize(aggVals.head), serialize(aggVals.tail.head))
} //end serializeFromListAggToListByteArray


val aggVals1 = List("text", 7.0D)
val se = s2(aggVals = aggVals1)

def d2() = {

  List(deserializeString(se.head), deserializeString(se.tail.head))
} //end serializeFromListAggToListByteArray

val d = d2

println(jsonString)

//def methodA(s: String) = s + "1"
//def methodAChar(s: Char) = s + "1"
//
//def f2() = "foo"
//def f = "bar"
//
//methodA(f2())
//methodAChar(f())
