//
//import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
//import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
//import java.time.LocalDateTime
//
//import scala.collection.mutable
//import java.time.format.DateTimeFormatter
//import java.util.{Properties, HashMap => JHashMap, Map => JMap}
//
//import _root_.scala.collection.JavaConverters._
//import scala.collection.JavaConversions._
//import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
//import com.lightbend.kafka.scala.streams.KStreamS
//import com.typesafe.config.Config
//import org.apache.kafka.clients.CommonClientConfigs
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.clients.producer.ProducerConfig
//import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
//import org.apache.kafka.common.serialization.Serdes
//import org.apache.kafka.streams.StreamsConfig
//import org.apache.kafka.streams.kstream.{ForeachAction, KStream, Predicate}
//import java.time.format.DateTimeParseException
//
//
//def serialize(value: Any): Array[Byte] = {
//  val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
//  val oos = new ObjectOutputStream(stream)
//  oos.writeObject(value)
//  oos.close()
//  stream.toByteArray
//}
//
//
//def deserialize[T](bytes: Array[Byte]):  Any = {
//  val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
//  val value = ois.readObject
//  ois.close()
//  value.asInstanceOf[T]
//}
//def convertByteArrayToJsonObject(ba: Array[Byte]):  JsonNode = {
//  val objectMapper = new ObjectMapper()
//  val node = objectMapper.readValue(ba, classOf[JsonNode])
//  node
//}
//
//def convertStringToJsonObject(str: String):  JsonNode = {
//  val objectMapper = new ObjectMapper()
//  val node = objectMapper.readValue(str, classOf[JsonNode])
//  node
//}
//
//def convertJsonToByteArray(obj: Object): Either[String, Array[Byte]] = {
//  var ba: Array[Byte] = Array.empty[Byte]
//  val out = new ByteArrayOutputStream()
//  try {
//    val mapper = new ObjectMapper()
//    mapper.writeValue(out, obj)
//    ba = out.toByteArray()
//  }
//  catch {
//    case e: Exception => {
//      Left("convertJsonToByteArray => can't convert record to byte array: " + e.fillInStackTrace())
//    }
//  }
//  finally {
//    out.close()
//  }
//  Right(ba)
//}
//
////val s = "{\"fields\":[{\"ben-acct-num\":\"28687008\"},{\"BEN_SVC_SRC_PRODT_ID\":\"8\"},{\"BEN_SVC_CTGRY_RANK\":\"0\"},{\"ben-nt-ctgry-cd\":\"0\"}]}"
////val map:mutable.HashMap[String, String] = mutable.HashMap[String, String]("1" -> "hi")
////
////val jMap:java.util.HashMap[String,String] = new JHashMap[String,String]
////jMap.put("a", s)
////val map:Map[String,String] = Map[String,String]("a" -> s)
////println(convertJsonToByteArray(jMap).right.toString)
//
//
////val sl: String = "{""fields"":[{""acct-num\":\"28687008\"},{\"cust-id\":\"8\"},{\"quantity\":\"1\"},{\"ord-count\":\"7\"}]}"
//val str: String = """{"fields":[{"acct-num":"28687008"},{"cust-id":"8"},{"quantity":"1"},{"ord-count":"7"}]}"""
//
//val s: String = "{\"fields\":[{\"ben-acct-num\":\"28687008\"},{\"BEN_SVC_SRC_PRODT_ID\":\"8\"},{\"BEN_SVC_CTGRY_RANK\":\"0\"},{\"ben-nt-ctgry-cd\":\"0\"}]}"
//val objectMapper = new ObjectMapper()
//
//val ba1: Array[Byte] = objectMapper.writeValueAsBytes(str)
//val node = objectMapper.readValue(ba1, classOf[JsonNode]).toString
//  val elem = objectMapper.readValue(ba1, classOf[JsonNode]).get("fields").findValue("acct-num").asText()
//val ba2 = serialize(s)
////val s1 = deserialize(ba1)
//val s2 = deserialize[String](ba2)
//val s3 = deserialize[String](serialize(s))
//val ss = serialize(s)
//val sss = convertByteArrayToJsonObject(ss).asInstanceOf[String]
//convertByteArrayToJsonObject(ss).toString
////convertStringToJsonObject().toString
////println(deserialize(convertJsonToByteArray(convertStringToJsonObject(s)).right.get).toString)
//
//
//import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//
//object JsonUtil {
//  val mapper = new ObjectMapper() with ScalaObjectMapper
//  mapper.registerModule(DefaultScalaModule)
//  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
//
//  def toJson(value: Map[Symbol, Any]): String = {
//    toJson(value map { case (k,v) => k.name -> v})
//  }
//
//  def toJson(value: Any): String = {
//    mapper.writeValueAsString(value)
//  }
//
//  def toMap[V: Manifest](json:String) = fromJson[Map[String,V]](json)
//
//  def fromJson[T: Manifest](json: String): T = {
//    mapper.readValue[T](json)
//  }
//}

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.{Map, Properties, UUID}

import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.scala.Serdes

val prop:Properties = new Properties()
prop.put("bootstrap.servers","192.168.1.100:9092,192.168.1.141:9092,192.168.1.113:9092,192.168.1.118:9092")
prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
prop.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

val consumer = new KafkaConsumer(prop)

val topics = List[String] ("my_topic_partition","my_topic_partition")

import collection.JavaConverters._

consumer.subscribe(topics.asJava)//(Consumed.`with`(Serdes.String, Serdes.ByteArray)
