name := "dataport-pecanstreet-etl"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1211.jre7"
  , "com.typesafe" % "config" % "1.3.1"
  , "com.typesafe.play" %% "play-json" % "2.6.2"
  , "com.typesafe.akka" %% "akka-typed" % "2.5.3"
  , "ch.qos.logback" % "logback-classic" % "1.1.9"
  , "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  , "com.typesafe.akka" %% "akka-slf4j" % "2.5.3"
  , "org.reactivemongo" %% "reactivemongo" % "0.12.5")

enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("lasalle.dataportpecanstreet.ETL")
