ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "seminarGraphx",
    idePackagePrefix := Some("de.seminar.graphx")
  )

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.4.0"