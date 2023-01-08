ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

val sparkVersion = "3.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "report-template"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.rogach" %% "scallop" % "4.1.0",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
  "com.typesafe" % "config" % "1.4.2"
)