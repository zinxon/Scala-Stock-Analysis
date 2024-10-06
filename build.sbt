ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "Scala",
    idePackagePrefix := Some("com.zinxon.spark")
  )

val sparkVersion = "3.5.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" %sparkVersion,
  "org.apache.spark" %% "spark-streaming" %sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" %sparkVersion,
  "org.apache.kafka" % "kafka-clients" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.19" % "test"
)

// https://mvnrepository.com/artifact/org.apache.spark/spark-sq"

