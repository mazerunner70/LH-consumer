
name := "LH-consumer"

version := "0.1"

scalaVersion := "2.13.2"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.5.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.5.0"



enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)

//https://medium.com/jeroen-rosenberg/lightweight-docker-containers-for-scala-apps-11b99cf1a666
mainClass in Compile := Some("core.Main")
dockerBaseImage       := "openjdk:jre"