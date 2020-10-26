package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {

  	val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaSparkCassandraAverage")
  	val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("file:///tmp/spark/checkpoint")

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

    val topicsSet = Set("avg")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topicsSet)

    val values = messages.map(message => message._2.split(","))
    val pairs = values.map(v => (v(0), v(1).toDouble))

    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
        val (sum, count) = state.getOption.getOrElse((0.0, 0))
        val newSum = value.getOrElse(0.0) + sum
        val newCount = count + 1
        state.update((newSum, newCount))
        (key, newSum/newCount)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
}
