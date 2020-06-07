package acme

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import _root_.scala.language.implicitConversions
import annotation.tailrec
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Map
import _root_.scala.language.implicitConversions
import _root_.scala.language.existentials
import io.confluent.common.utils.TestUtils
import java.util.{Map, Properties, UUID}
import acme.config.GroupByConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes => JSerdes, _}
import org.apache.kafka.streams.kstream.{KTable => KTableJ, _}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import acme.config.GroupByConfig
import acme.GroupByAction
import acme.AggContext2
import acme._

@RunWith(classOf[JUnitRunner])
class KafkaSuite extends FunSuite {



}
