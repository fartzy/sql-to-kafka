package acme

import java.util.{Properties, UUID}
import io.confluent.common.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import com.typesafe.config.ConfigFactory
import _root_.scala.language.implicitConversions
import _root_.scala.collection.JavaConverters._
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{JsonNodeFactory}
import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
import _root_.scala.collection.mutable.ListBuffer


object JoinAction {
//
//    def initializeStream(builder: StreamsBuilder,
//                             rekeyNeeded: Boolean,
//                             fieldList: List[String],
//                             topicName: String,
//                             rekeyTopicName: String): KStream[String, Array[Byte]] = {
//
//        implicit val c = Consumed.`with`(Serdes.String(), Serdes.ByteArray())
//        implicit val p = Produced.`with`(Serdes.String(),Serdes.ByteArray())
//
//        val stream: KStream[String, Array[Byte]] = builder.stream(topicName)
//
//        val outputStream = if(rekeyNeeded) {
//            stream.selectKey{ (key: String, value: Array[Byte]) => {
//                val lv = JoinUtils.convertByteArrayToJsonObject(value)
//                val lst = new ListBuffer[String]
//                JoinUtils.reKey(lv, fieldList, lst)
//                val newKey = lst.mkString("-")
//                newKey
//            }}.to(rekeyTopicName)
//            builder.stream(rekeyTopicName)
//        } else {
//            stream
//        }
//        outputStream
//    }
//
//    def main(args: Array[String]) {
//        val value = System.getenv("BOOTSTRAP_SERVERS")
//        val appConfigFile = System.getenv("APPLICATION_CONFIG_FILE")
//
//        val config = ConfigFactory.load(appConfigFile)
//        val bootStrapServers = if(value == null) config.getString("kafka.connection.bootstrap.servers") else value
//
//        val streamingConfig = {
//            val settings = new Properties
//            val appId = UUID.randomUUID.toString
//            settings.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
//            settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
//            settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
//            settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.ByteArraySerde])
//            settings.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory.getAbsolutePath)
//            settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//            settings
//        }
//convertByteArrayToJsonObject(ss)convert
//        // Read in all the necessary configuration data that will be used
//        // to create the streams topology
//        //-------------------------------------------------------------------
//        val joinStep = config.getConfigList("joinSteps").get(0)
//        val leftTopicName = joinStep.getString("topic.left")
//        val rightTopicName = joinStep.getString("topic.right")
//        val outputTopic = joinStep.getString("topic.out")
//        val filterExpression = joinStep.getStringList("postjoin.filter")
//        val postFilteringNeeded = joinStep.getBoolean("postFilter.needed")
//        val filterTopic = joinStep.getString("topic.filter")
//        val outputFields = joinStep.getStringList("join.outputFields")
//        val extraFields = joinStep.getStringList("join.extraFields")
//        val rekeyLeftNeeded = joinStep.getBoolean("rekey.leftNeeded")
//        val rekeyRightNeeded = joinStep.getBoolean("rekey.rightNeeded")
//        val rekeyTopicLeft = joinStep.getString("rekeyTopic.left")
//        val rekeyTopicRight = joinStep.getString("rekeyTopic.right")
//        val leftRekeyColumnList = if(rekeyLeftNeeded) joinStep.getStringList("joinOn.leftFields").asScala.toList else List.empty
//        val rightRekeyColumnList = if(rekeyRightNeeded) joinStep.getStringList("joinOn.rightFields").asScala.toList else List.empty
//
//        // Output debug information - DEBUG ONLY
//        //----------------------------------------------------
//        println(s"Bootstrap Servers = ${bootStrapServers}")
//        println(s"Left Topic = ${leftTopicName}")
//        println(s"Right Topic = ${rightTopicName}")
//        println(s"Output Topic = ${outputTopic}")
//        println(s"Post Join Filter = ${filterExpression}")
//
//        if(postFilteringNeeded) {
//            if(!JoinUtils.validateFilterExpression(filterExpression.asScala.toList)) {
//                throw new IllegalArgumentException("Malformed filter expression")
//            }
//        }
//
//        val builder = new StreamsBuilderS
//
//        // Initialize the left and right streams based on the information obtained from
//        // the configuration file
//        //--------------------------------------------------------------------------------
//        val left = initializeStream(builder,
//                                    rekeyLeftNeeded,
//                                    leftRekeyColumnList,
//                                    leftTopicName,
//                                    rekeyTopicLeft)
//
//        val right = initializeStream(builder,
//                                     rekeyRightNeeded,
//                                     rightRekeyColumnList,
//                                     rightTopicName,
//                                     rekeyTopicRight)
//
//        // Implicits which are needed by the join and filter transformations
//        //---------------------------------------------------------------------------
//        implicit val j = Joined.`with`(Serdes.String(), Serdes.ByteArray(), Serdes.ByteArray())
//        implicit val c = Consumed.`with`(Serdes.String(), Serdes.ByteArray())
//        implicit val p = Produced.`with`(Serdes.String(),Serdes.ByteArray())
//
//        val outTopic = if(postFilteringNeeded) filterTopic else outputTopic
//
//        // This stream will be used only if post join filtering transformation is
//        // needed in the pipeline
//        //---------------------------------------------------------------------------
//        val finalStream: KStreamS[String, Array[Byte]] = builder.stream(filterTopic)
//
//        val joinedStream = left.join(right, (l: Array[Byte], r: Array[Byte]) => {
//                                    val lv = JoinUtils.convertByteArrayToJsonObject(l)
//                                    val rv = JoinUtils.convertByteArrayToJsonObject(r)
//
//                                    // Create a new JSON object from picking fields from
//                                    // the left and right topics.
//                                    //------------------------------------------------------
//                                    val instant = Instant.now
//                                    val date = DateTimeFormatter.ISO_INSTANT.format(instant)
//                                    val uuid = UUID.randomUUID.toString
//                                    val rootNode = JsonNodeFactory.instance.objectNode
//                                    rootNode.put("parentUUID", "")
//                                    rootNode.put("UUID", uuid)
//                                    rootNode.put("type", "action")
//                                    rootNode.put("createdAt", date)
//                                    val node = JsonNodeFactory.instance.objectNode
//                                    JoinUtils.joinValues(lv, rv, node, outputFields.asScala.toList)
//                                    rootNode.set("payload", node)
//                                    val mapper = new ObjectMapper
//                                    val bytes = mapper.writeValueAsBytes(rootNode)
//                                    bytes
//                        }, JoinWindows.of(TimeUnit.MINUTES.toMillis(5))).to(outTopic)
//
//        if(postFilteringNeeded) {
//            finalStream.filter((key: String, value: Array[Byte]) => {
//                val lv = JoinUtils.convertByteArrayToJsonObject(value)
//                val filterPredicate = JoinUtils.buildPostJoinPredicate(filterExpression.asScala.toList, lv)
//                val rc = if(filterPredicate.test(key, lv)) true else false
//                rc
//            }).mapValues((value: Array[Byte]) => {
//                // Once the post filtering transformation is done, remove all extra fields that
//                // was placed in the output topic to facilitate the post filtering transformation
//                //-----------------------------------------------------------------------------------
//                val lv = JoinUtils.convertByteArrayToJsonObject(value)
//                val ba: Array[Byte] = JoinUtils.removeEntryFromJsonNode(extraFields.asScala.toList, value)
//                //val rc = JoinUtils.convertByteArrayToJsonObject(ba)
//                ba
//            }).to(outputTopic)
//        }
//
//        // Following foreach block is for testing purposes only.
//        // Use it to validate messages sent to the output topic
//        //-------------------------------------------------------
//        /* joinedStream.foreach((k: String, v: Array[Byte]) => {
//                val lv = JoinUtils.convertByteArrayToJsonObject(v)
//                println(s"${k}: ${lv}")
//            }) */
//
//        // Create the streams topology from the streams configuration
//        //-----------------------------------------------------------
//        val streams = new KafkaStreams(builder.build(), streamingConfig)
//
//        // Initialize exception handler for any uncaught exceptions
//        //--------------------------------------------------------------
//        streams.setUncaughtExceptionHandler((thread: Thread, t: Throwable) => {
//            println("Caught exception: " + t)
//            t.printStackTrace()
//            println("Shutting down streams topology....")
//            streams.close()
//        })
//
//        println("Cleaning up from prior run...")
//        streams.cleanUp()
//
//        println("Starting the streams topology...")
//        streams.start()
//
//        Runtime.getRuntime.addShutdownHook(new Thread {
//            override def run(): Unit = {
//                println("Gracefully shutting down the streams.")
//                streams.close()
//            }
//        })
//    }
}
