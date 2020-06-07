package acme

import java.util.{Properties, UUID}
import io.confluent.common.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import com.typesafe.config.ConfigFactory
import _root_.scala.language.implicitConversions
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.ByteArrayOutputStream
import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
//import org.apache.kafka.streams.scala._
import _root_.scala.collection.mutable

object FilterAction {

    def reKey(lv: JsonNode, lst: List[String], out: mutable.ListBuffer[String]): String = lst match {
        case Nil => ""
        case h :: t => {
            val s = lv.get(h)
            out.append(s.asText)
            out.toString() + reKey(lv, t, out)
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

    def main(args: Array[String]) {
        val bootStrapServers = "localhost:9092"

        val streamingConfig = {
            val settings = new Properties
            val appId = UUID.randomUUID.toString
            settings.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
            settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
            settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.ByteArraySerde])
            settings.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
            settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            settings
        }

        val config = ConfigFactory.load()

        val filterStep = config.getConfigList("filterSteps").get(0)
        val inputTopic = filterStep.getString("topic.in")
        val outputTopic = filterStep.getString("topic.out")
        val filterColumnList = filterStep.getStringList("filter.columns")

        val builder = new StreamsBuilder

        implicit val c = Consumed.`with`(Serdes.String(), Serdes.ByteArray())
        val stream: KStream[String, Array[Byte]] = builder.stream(inputTopic)

        println(s"Input Topic = ${inputTopic}")
        println(s"Output Topic = ${outputTopic}")
        println(s"Filter Column(s) = ${filterColumnList}")

        implicit val s = Serialized.`with`(Serdes.String(), Serdes.ByteArray())
        implicit val p = Produced.`with`(Serdes.String(),Serdes.String())

        val t = stream.filter((key: String, value: Array[Byte]) => {
            val lv = FilterAction.convertByteArrayToJsonObject(value)
            val col = filterColumnList.get(0)
            val str = lv.get(col).asText
            true
        })

        // Write completed transformation to final output topic
        //------------------------------------------------------
        // stream.to(outputTopic)

        // Following foreach block is for testing purposes only
        //-------------------------------------------------------
        /*leftTable.toStream.foreach((k: String, v: Array[Byte]) => {
                val lv = JoinAction.convertByteArrayToJsonObject(v)
                println(s"${k}: ${lv}")
            })*/

        // Create the streams topolgy from the streams configuration
        //-----------------------------------------------------------
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

/*
val lst = new ListBuffer[String]
            GroupByAction.reKey(lv, filterColumnList.asScala.toList, lst)
            val newKey = lst.mkString("-")
            (newKey, value)
            val newKey = FilterAction.reKey(lv, filterColumnList.asScala.toList)
            (newKey, value)
 */
