name := "dataport-pecanstreet-etl"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1211.jre7"
  , "com.typesafe" % "config" % "1.3.1"
  , "com.typesafe.play" %% "play-json" % "2.5.8"
  , "com.typesafe.akka" %% "akka-actor" % "2.4.11"
  , "com.typesafe.akka" %% "akka-stream" % "2.4.11"
  , "org.slf4j" % "slf4j-api" % "1.7.21"
  , "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  , "ch.qos.logback" % "logback-classic" % "1.1.7"
  , "org.reactivemongo" % "reactivemongo_2.11" % "0.11.14")