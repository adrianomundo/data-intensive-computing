
name := "spark_kafka"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.1",
  "io.confluent" % "kafka-avro-serializer" % "3.0.0",
  ("com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2").exclude("io.netty", "netty-handler"),
  ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0").exclude("io.netty", "netty-handler"),
  "org.apache.spark" %% "spark-mllib" % "2.2.1" % "compile",
  "org.elasticsearch" % "elasticsearch" % "7.2.0",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.2.0"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Confluent Maven Repo" at "https://packages.confluent.io/maven/"
)
