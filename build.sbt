name := "tecoloco"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.scalatest" %% "scalatest" % "3.2.16",
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.2",
  "org.apache.hadoop" % "hadoop-common" % "3.3.2",
  "org.apache.hadoop" % "hadoop-client" % "3.3.2",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.477",
  "org.apache.spark" %% "spark-hadoop-cloud" % "3.3.2",
  "org.postgresql" % "postgresql" % "42.6.0"
)
