package acme

import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ListBuffer
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.FunctionConversions._
import org.apache.kafka.streams.scala.kstream.KStream

import _root_.scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import reflect.runtime.universe._
import scala.reflect.api._
import com.lightbend.kafka.scala.streams.{KTableS, StreamsBuilderS}

import scala.annotation.tailrec
import acme.util._
import com.typesafe.config.ConfigFactory

import _root_.scala.language.implicitConversions
import _root_.scala.language.existentials
import io.confluent.common.utils.TestUtils
import java.util.{Map, Properties, UUID}

import acme.config.GroupByConfig
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes => JSerdes, _}
import org.apache.kafka.streams.kstream.{KTable => KTableJ, _}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._


object GroupByAction {

  def main(args: Array[String]) {
    val bootStrapServers = "localhost:9092"

    val streamingConfig = {
      val settings = new Properties
      val appId = UUID.randomUUID.toString
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
     // settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
      settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[JSerdes.StringSerde])
      settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[JSerdes.ByteArraySerde])
      settings.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
      settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      settings
    }

    val groupByConfig = new GroupByConfig
    val thisAgg = AggContext.instance(groupByConfig)

    val inputTopic = groupByConfig.inputTopic
    val outputTopic = groupByConfig.outputTopic
    val groupByColumnList = thisAgg.getListGroupByColName
    val aggregateColumnList = thisAgg.getListValToAgg
    val aggregateOperationList = groupByConfig.aggregateOperationList


    val builder = new StreamsBuilder
    val ktable: KTable[String, Array[Byte]] = builder.table[String, Array[Byte]](inputTopic)(Consumed.`with`(Serdes.String, Serdes.ByteArray))

    println(s"Input Topic = ${inputTopic}")
    println(s"Output Topic = ${outputTopic}")
    println(s"GroupBy Column(s) = ${groupByColumnList}")
    println(s"Aggregate Column(s) = ${aggregateColumnList}")
    println(s"Aggregate Operation(s) = ${aggregateOperationList}")

    val result = ktable
      .groupBy((k,v) => {

        val newKey = thisAgg.groupByKey(v)._2
        val valueNew = thisAgg.serializeFromListByteArraytoByteArray(
          thisAgg.serializeFromListAggToListByteArray(
            thisAgg.getListValsToAgg(v)._2))

        (newKey, valueNew)

      })(Serialized.`with`(Serdes.String, Serdes.ByteArray))
      /*
       * aggregate will use the SAM syntax to create an Adder and a Subtractor aggregator
       * The Adder will add take the value and aggregate it to the agg
       * The Subtractor will remove the old value from the agg
       * The `adder` method inside the Aggregation object is where the Adder operation takes place
       * The `subtractor` method inside the Aggregation object is where the Subtractor operation takes place
       */
      .aggregate[Array[Byte]](thisAgg.serializeFromListByteArraytoByteArray(
      thisAgg.serializeFromListAggToListByteArray(
        thisAgg.initialValues())
    )
    )((key: String
       ,newValue: Array[Byte]
       ,agg: Array[Byte]) => {
      thisAgg.aggregateAdder(key, newValue, agg)
    },
      (key: String
       ,oldValue: Array[Byte]
       ,agg: Array[Byte]) => {
        thisAgg.aggregateSubtractor(key, oldValue, agg)
      })(Materialized.`with`(Serdes.String, Serdes.ByteArray))
      /*
       * mapValues will do a final operation for something like an average, median or variance
       * An operation like a sum will just do nothing
       * The `valueMapper` method inside the Aggregation object is where the final operation takes place
       */
      .mapValues( v => { val t = thisAgg
        .deserializeFromListByteArrayToListAgg(
        isAgg = true
        ,thisAgg.deserializeToListByteArrayFromByteArray(
          thisAgg.valueMapper(v)
          )
        )
        .map{ case v => println(v); v.toString }

        thisAgg.returnJsonNode(t).toString})
      .toStream
      .to(outputTopic)

    val streams = new KafkaStreams(builder.build(), streamingConfig)

    // Initialize exception handler for any uncaught exceptions
    //--------------------------------------------------------------
    streams.setUncaughtExceptionHandler((thread: Thread, t: Throwable) => {
      println("Caught exception: " + t)
      t.printStackTrace()
      println("Shutting down streams topology....")
      streams.close()
    })

    println("Cleaning up from prior run...")
    streams.cleanUp()

    println("Starting the streams topology...")
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        println("Gracefully shutting down the streams.")
        streams.close()
      }
    })
  }
}
