ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "composant_4_storage",
    libraryDependencies ++= Seq(
      // Kafka consumer fonctionnel avec FS2
      "com.github.fd4s" %% "fs2-kafka" % "3.1.0",

      // JSON avec Circe
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",

      // Hadoop HDFS client
      "org.apache.hadoop" % "hadoop-client" % "3.3.6",
      "org.apache.hadoop" % "hadoop-hdfs" % "3.3.6",

      // Logging silencieux
      "org.slf4j" % "slf4j-simple" % "2.0.13"
    )
  )