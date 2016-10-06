name := "dataport-pecanstreet-etl"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1211.jre7"
  , "com.typesafe" % "config" % "1.3.1"
  , "com.typesafe.play" %% "play-json" % "2.5.8")