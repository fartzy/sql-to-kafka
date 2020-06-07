package acme

import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.scalatest.{Matchers, WordSpec}
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KeyValue, StreamsBuilder => JStreamsBuilder}
import org.apache.kafka.streams.kstream.{Consumed, KeyValueMapper, Produced, Serialized, KStream => JKStream, KTable => JKTable}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


class KafkaEmbeddedExampleSpec
    extends WordSpec
    with Matchers
    with EmbeddedKafkaStreamsAllInOne {

  import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder

  implicit val config =
    EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

  val (inTopic, outTopic) = ("in", "out")

  val stringSerde: Serde[String] = Serdes.String()

  "A Kafka streams test" should {
    "be easy to run with streams and consumer lifecycle management" in {
      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic)(Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic)(Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")
        publishToKafka(inTopic, "baz", "yaz")
        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] =
            consumer.consumeLazily(outTopic)
          consumedMessages.take(2) should be(
            Seq("hello" -> "world", "foo" -> "bar"))
          consumedMessages.drop(2).head should be("baz" -> "yaz")
        }
      }
    }


      "be grouped properly" in {
        val streamBuilder = new JStreamsBuilder
//        val kstream: JKStream[String, String] =
//          streamBuilder.stream[String, String](inTopic, Consumed.`with`(stringSerde, stringSerde))
//              .groupBy(new KeyValueMapper[String, String, KeyValue[String, String]] {
//                   def apply(k: String,v: String) = {new KeyValue[String,String](k,v)} })
//            .count
//            .toStream()
            //.to(outTopic) Produced.`with`(stringSerde, stringSerde))

//        table[String, String](inTopic, Consumed.`with`(stringSerde, stringSerde))
////        val stream: KStream[String, String] =
////          streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))
//
//        val p = Produced.`with`(stringSerde, stringSerde)
//
//        ktable.groupBy[String, String](new KeyValueMapper[String, String, KeyValue[String, String]] {
//          def apply(k: String,v: String) = {new KeyValue[String,String](k,v)} })
//          .count
//          .toStream()
//          .to(outTopic) Produced.`with`(stringSerde, stringSerde))

        runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
          publishToKafka(inTopic, "key1", "1")
          publishToKafka(inTopic, "key1", "2")
          publishToKafka(inTopic, "key1", "3")
          publishToKafka(inTopic, "key2", "4")
          publishToKafka(inTopic, "key2", "5")
          publishToKafka(inTopic, "key3", "6")
          withConsumer[String, String, Unit] { consumer =>
            val consumedMessages: Stream[(String, String)] =
              consumer.consumeLazily(outTopic)
            consumedMessages.take(2) should be(
              Seq("hello" -> "3", "foo" -> "2"))
            consumedMessages.drop(2).head should be("baz" -> "1")
          }
        }
      }


    "allow support creating custom consumers" in {
      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic)(Consumed.`with`(Serdes.String, Serdes.String))

      stream.to(outTopic)(Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")
        val consumer = newConsumer[String, String]()
        consumer.consumeLazily[(String, String)](outTopic).take(2) should be(
          Seq("hello" -> "world", "foo" -> "bar"))
        consumer.close()
      }
    }

    "allow for easy string based testing" in {
      val streamBuilder = new JStreamsBuilder
      val stream: JKStream[String, String] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreamsWithStringConsumer(Seq(inTopic, outTopic),
        streamBuilder.build()) { consumer =>
        publishToKafka(inTopic, "hello", "world")
        consumer.consumeLazily[(String, String)](outTopic).head should be(
          "hello" -> "world")
      }
    }
  }
}
