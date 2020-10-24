import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object Consumer {
  def main(args: Array[String]) {

    /*            SPARK CONTEXT           */

    val conf = new SparkConf().setAppName("csv").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("MyApp").getOrCreate()
    ssc.checkpoint("./checkpoint")

    val mySchema = StructType(Seq(
      StructField("Time", DoubleType, true),
      StructField("V1", DoubleType, true),
      StructField("V2", DoubleType, true),
      StructField("V3", DoubleType, true),
      StructField("V4", DoubleType, true),
      StructField("V5", DoubleType, true),
      StructField("V6", DoubleType, true),
      StructField("V7", DoubleType, true),
      StructField("V8", DoubleType, true),
      StructField("V9", DoubleType, true),
      StructField("V10", DoubleType, true),
      StructField("V11", DoubleType, true),
      StructField("V12", DoubleType, true),
      StructField("V13", DoubleType, true),
      StructField("V14", DoubleType, true),
      StructField("V15", DoubleType, true),
      StructField("V16", DoubleType, true),
      StructField("V17", DoubleType, true),
      StructField("V18", DoubleType, true),
      StructField("V19", DoubleType, true),
      StructField("V20", DoubleType, true),
      StructField("V21", DoubleType, true),
      StructField("V22", DoubleType, true),
      StructField("V23", DoubleType, true),
      StructField("V24", DoubleType, true),
      StructField("V25", DoubleType, true),
      StructField("V26", DoubleType, true),
      StructField("V27", DoubleType, true),
      StructField("V28", DoubleType, true),
      StructField("Amount", DoubleType, true),
      StructField("Class", DoubleType, true)
    ))

    /*         END OF SPARK CONTEXT        */
    /*            KAFKA CONSUMER           */

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val topicSet = Set("credit-transactions")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topicSet)

    /*         END OF KAFKA CONSUMER        */
    /*         CASSANDRA CONNECTION         */
    /*

// Cassandra initialization
val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
val session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS credit WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};")
session.execute("CREATE TABLE IF NOT EXISTS credit.transactionscazzi (time int PRIMARY KEY, amount int);")
session.close()

def cleanTransaction(transaction: Array[String]): Array[Int] = {
  // keep only: 0-> transaction time; 29->transaction amount; 30->class (fraudulent or not)
  //TO BE READDED Array(transaction(0).toInt, transaction(29).toInt, transaction(30).toInt)
  Array(transaction(0).toInt, transaction(29).toInt)
}

val usefulData = values.map(pair => cleanTransaction(pair))
// TO BE READDED usefulData.saveToCassandra("credit", "transactions", SomeColumns("time", "amount", "class"))
//usefulData.saveToCassandra("credit", "transactionscazzi", SomeColumns("time", "amount"))
usefulData.write.mode("overwrite").cassandraFormat("transactionscazzi", "credit").save()

 */
    /*      END OF CASSANDRA CONNECTION     */
    /*           STREAM PROCESSING          */

    val values = messages.map(message => {
      val myDataFirst = message._2.split(",")
      val myData = myDataFirst.map(_.toDouble)
      println("TIME: " + myData(0) + " - tipo: " + myData(0).getClass)
      println("ALTRE COSE: " + myData(1) + " - tipo: " + myData(1).getClass)
      println("AMOUNT: " + myData(29) + " - tipo: " + myData(29).getClass)
      println("CLASS: " + myData(30) + " - tipo: " + myData(30).getClass)
      myData
    })

    values.print()

    val modelPath = "/home/fonzie/IdeaProjects/SBTPScalaProjectV2/src/main/scala/Consumer"
    val model = PipelineModel.read.load(modelPath)

    values.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val rowRDD = rdd.map(a => Row.fromSeq(a))
        val myDF = spark.createDataFrame(rowRDD, mySchema)

        println("ECCHECCAZZOPRIMA")
        myDF.show(1)
        println("ECCHECCAZZODOPO")

        val result = model.transform(myDF)
        result.show(1)
        println("POSTPREDICTION")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
