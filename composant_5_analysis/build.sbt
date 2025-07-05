ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "composant_5_analysis",
    libraryDependencies ++= Seq(
      // Spark avec Hadoop intégré
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      
      // Logging control
      "org.slf4j" % "slf4j-simple" % "2.0.13",
      "log4j" % "log4j" % "1.2.17"
    ),
    // Éviter les conflits de dépendances
    dependencyOverrides ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "3.3.4",
      "org.apache.hadoop" % "hadoop-common" % "3.3.4"
    ),
    // Supprimer les logs au niveau SBT
    run / javaOptions ++= Seq(
      "-Dlog4j.configuration=log4j.properties",
      "-Dspark.ui.showConsoleProgress=false"
    )
  )