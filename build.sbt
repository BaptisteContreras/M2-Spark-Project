name := "SparkTPApp"

version := "1.0"

scalaVersion := "2.11.8"

sbtVersion := "1.2.4"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8") 

// Nécessaire pour les tests Spark car un seul contexte de test Spark peut être actif
parallelExecution in Test := false

