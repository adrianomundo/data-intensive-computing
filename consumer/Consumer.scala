import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.SomeColumns
import org.apache.commons.codec.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.kafka.common.serialization.StringDeserializer

object Consumer {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreditCardsFraudDetector")
    // Streaming data will be divided into batches every 1 second
    val streamingContext = new StreamingContext(conf, Seconds(1))

    streamingContext.checkpoint("file:///tmp/spark/checkpointForCreditCardsFraudDetector")


    // Cassandra initialization
    //val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    //val session = cluster.connect()

    //session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};")
    //session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")
    //session.close()


    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val topicsSet = Set("transactions")
    val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](streamingContext, kafkaConf, topicsSet)
    

    //val messages= KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
  //streamingContext,
  //PreferConsistent,
  //Subscribe[String, String](kafkaParams,topicsSet)
//)
    
    //println(messages)	

    // split values of the messages upon the comma
    val values = messages.map(message => message._2)
    println(values)
    val pairs = values.map(row => return (row(0), row(1)))

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
    }

}

