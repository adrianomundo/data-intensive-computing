import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import scala.util.parsing.json._
import scala.collection.mutable

object Consumer {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("csv").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint("./checkpoint")

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val topicSet = Set("credit-transactions")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topicSet)
    
    val values = messages.map(message => message._2.split(",")))
    values.print()


    ssc.start()
    ssc.awaitTermination()
    /*
    // Cassandra initialization
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")
    session.close()


    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val topicSet = Set("transactions")
    //val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](streamingContext, kafkaConf, topicsSet)


    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaConf, topicsSet)

    println(messages)

    // split values of the messages upon the comma
    val values = messages.map(message => message._2)
    println(values)
    //val pairs = values.map(row => return (row(0), row(1)))

    // Here I'm wasting all useless information for the visualization purpose
    //val usefulData = pairs.map(pair => cleanTransaction(pair))

    //println(usefulData)

    //val stateDStream = usefulData

    //stateDStream.saveToCassandra("credit-space", "credit-transactions-frauds", SomeColumns("time", "amount", "class"))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def cleanTransaction(transaction: Array[String]): Array[Int] = {
    // keep only: 0-> transaction time; 29->transaction amount; 30->class (fraudulent or not)
    Array(transaction(0).toInt, transaction(29).toInt, transaction(30).toInt)

 */
  }

}



