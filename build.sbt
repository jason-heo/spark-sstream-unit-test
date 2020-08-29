lazy val root = (project in file(".")).
  settings(
    version := "0.1",
    scalaVersion := "2.11.8"
  )

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.6.4",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
)

assemblyJarName in assembly := "spark-sstream-unit-test.jar"

assemblyMergeStrategy in assembly := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
