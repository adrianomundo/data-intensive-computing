import java.io.{BufferedReader, FileReader}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object Producer extends App {
  val path: String = "./creditcard.csv"
  val topic: String = "credit-transactions"
  var i: Int = 0
  val brokers: String = "localhost:9092"
  val bufferedReader: BufferedReader = loadStreamInJava(path)

  def loadStreamInJava(pathToCSV: String): BufferedReader = {
    new BufferedReader(new FileReader(pathToCSV))
  }

  def sendAllDataInJava(): Unit = {
    var line = ""
    while ( {line = bufferedReader.readLine(); line != null}) {

     val data = new ProducerRecord[String, String](topic, i.toString, line)
      i += 1
      producer.send(data)
      println(data)
      try Thread.sleep(1000)
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "CreditCardsTransactionsProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  sendAllDataInJava()

  producer.close()
}
