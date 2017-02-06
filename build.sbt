name := "dataport-pecanstreet-etl"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1211.jre7"
  , "com.typesafe" % "config" % "1.3.1"
  , "com.typesafe.play" %% "play-json" % "2.5.8"
  , "ch.qos.logback" % "logback-classic" % "1.1.9"
  , "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  , "org.reactivemongo" % "reactivemongo_2.11" % "0.11.14")

enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("lasalle.dataportpecanstreet.ETL")
