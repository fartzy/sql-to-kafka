package acme
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization._
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.ExecutionException
import acme.TestConstants
import org.scalatest.FunSuite

import scala.collection.JavaConverters._



class TestProducer  extends FunSuite{

//  def main(args: Array[String]) = {
//    runProducer
//  }

  def createProducer: Producer[String, Array[Byte]] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestConstants.KAFKA_BROKERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, TestConstants.CLIENT_ID)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer].getName)
    //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
    val producer = new KafkaProducer[String, Array[Byte]](props)
    producer
  }




  def runProducer(): Unit = {
    val producer = createProducer
    val colObj1 = new acme.MaxAggregation2("test-column-1", "test-column-output-1")
//    val colObj1 = new acme.MaxAggregation("test-column-1")
    val testRecords = TestConstants.TEST_RECORDS
    for(i <- 1 to testRecords.length) {
      val record = new ProducerRecord(TestConstants.INGEST_TOPIC, i+ "",AggContext2.convertJsonToByteArray(Map("" + i -> testRecords(i))).right.get)
      try {
        val metadata: RecordMetadata = producer
          .send(record)
          .get
        System.out.println("Record sent with key " + i + " to partition " + metadata.partition + " with offset " + metadata.offset)
      } catch {
        case e: ExecutionException =>
          System.out.println("Error in sending record")
          System.out.println(e)
        case e: InterruptedException =>
          System.out.println("Error in sending record")
          System.out.println(e)
      }
    }
  }

  test ("check if sending to topic " + TestConstants.INGEST_TOPIC) {
    runProducer
  }

}
