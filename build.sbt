name := "lastfm-exercise"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"
val scalaTestVersion = "3.0.5"
val log4j2Version = "2.11.0"
val log4j2ScalaApiVersion = "11.0"

libraryDependencies ++= List(
  "org.apache.logging.log4j" % "log4j-api" % log4j2Version,
  "org.apache.logging.log4j" % "log4j-core" % log4j2Version,
  "org.apache.logging.log4j" %% "log4j-api-scala" % log4j2ScalaApiVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)