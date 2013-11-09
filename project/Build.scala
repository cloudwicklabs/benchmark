import sbt._
import Keys._

object Build extends Build {
  val ScalaVersion = "2.10.3"

  lazy val root = Project("benchmark", file(".")) settings(
      version := "0.1",
      scalaVersion := ScalaVersion,
      organization := "com.cloudwick",
      scalacOptions ++= Seq("-unchecked", "-deprecation"),
      libraryDependencies ++= Dependencies.compile,
      libraryDependencies ++= Dependencies.testDependencies
    )

  object Dependencies {
    val compile = Seq(
      "org.mongodb" %% "casbah" % "2.6.3",
      "commons-logging" % "commons-logging" % "1.1.1",
      "org.slf4j" % "slf4j-simple" % "1.7.5",
      "org.slf4j" % "slf4j-api" % "1.7.5",
      "com.github.scopt" %% "scopt" % "3.1.0",
      "org.apache.solr" % "solr-solrj" % "4.3.1",
      "jp.sf.amateras.solr.scala" %% "solr-scala-client" % "0.0.8"
    )

    val testDependencies = Seq(
      "org.specs2" %% "specs2" % "1.14" % "test",
      "org.mockito" % "mockito-all" % "1.9.0" % "test",
      "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
    )
  }
}