import com.datastax.driver.core.Cluster
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions._


object Consumer {
  def main(args: Array[String]) {
    /*            SPARK CONTEXT           */

    val conf = new SparkConf().setAppName("csv").setMaster("local")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("MyApp").getOrCreate()
    ssc.checkpoint("./checkpoint")

    val mySchema = StructType(Seq(
      StructField("id", DoubleType, true),
      StructField("V1", DoubleType, true), StructField("V2", DoubleType, true), StructField("V3", DoubleType, true),
      StructField("V4", DoubleType, true), StructField("V5", DoubleType, true), StructField("V6", DoubleType, true),
      StructField("V7", DoubleType, true), StructField("V8", DoubleType, true), StructField("V9", DoubleType, true),
      StructField("V10", DoubleType, true), StructField("V11", DoubleType, true), StructField("V12", DoubleType, true),
      StructField("V13", DoubleType, true), StructField("V14", DoubleType, true), StructField("V15", DoubleType, true),
      StructField("V16", DoubleType, true), StructField("V17", DoubleType, true), StructField("V18", DoubleType, true),
      StructField("V19", DoubleType, true), StructField("V20", DoubleType, true), StructField("V21", DoubleType, true),
      StructField("V22", DoubleType, true), StructField("V23", DoubleType, true), StructField("V24", DoubleType, true),
      StructField("V25", DoubleType, true), StructField("V26", DoubleType, true), StructField("V27", DoubleType, true),
      StructField("V28", DoubleType, true),
      StructField("Amount_Scaled", DoubleType, true),
      StructField("Time_Scaled", DoubleType, true),
      StructField("Label", DoubleType, true)
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

    // Cassandra initialization
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS credit WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE TABLE IF NOT EXISTS credit.transactions (id double PRIMARY KEY, label double, prediction double)")

    session.close()

    /*      END OF CASSANDRA CONNECTION     */
    /*           STREAM PROCESSING          */

    val values = messages.map(message => {
      val myDataFirst = message._2.split(",")
      val myData = myDataFirst.map(_.toDouble)
      myData
    })

    val modelPath = "./LRModelSpark"
    val model = LogisticRegressionModel.load(modelPath)

    values.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val rowRDD = rdd.map(a => Row.fromSeq(a))
        val myDF = spark.createDataFrame(rowRDD, mySchema)
        val DFwhithoutId = myDF.drop("label")//.drop("id")
        val labelDF = myDF.drop("V1","V2","V3","V4","V5","V6","V7","V8","V9","V10",
          "V11","V12","V13","V14","V15","V16","V17","V18","V19","V20",
          "V21","V22","V23","V24","V25","V26","V27","V28","Amount_Scaled","Time_Scaled","id")
          .withColumn("rowId1", monotonically_increasing_id())

        val assembler = new VectorAssembler()
          .setInputCols(Array("V1","V2","V3","V4","V5","V6","V7","V8","V9","V10",
            "V11","V12","V13","V14","V15","V16","V17","V18","V19","V20",
            "V21","V22","V23","V24","V25","V26","V27","V28","Amount_Scaled","Time_Scaled"))
          .setOutputCol("features")
        val featuresDF = assembler
          .transform(DFwhithoutId)
          .drop("V1","V2","V3","V4","V5","V6","V7","V8","V9","V10",
            "V11","V12","V13","V14","V15","V16","V17","V18","V19","V20",
            "V21","V22","V23","V24","V25","V26","V27","V28","Amount_Scaled","Time_Scaled"
          )

        val featuresDFforJoin = featuresDF
          .withColumn("rowId2", monotonically_increasing_id())

        val df = featuresDFforJoin.as("df1").join(labelDF.as("df2"), featuresDFforJoin("rowId2") === labelDF("rowId1"), "inner").select("df1.features", "df1.id", "df2.label")

        val result = model.transform(df)
        result.show()

        /*        STORE INTO CASSANDRA      */

        val dataForCassandra = result.drop("features", "rawPrediction", "probability")
        dataForCassandra.write.mode("append").cassandraFormat("transactions", "credit").save()

      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
