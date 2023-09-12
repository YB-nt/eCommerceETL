name := "PreDataProcessing"

version := "0.1"

scalaVersion := "2.12.18"

organization :="com.github.ybnt"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.postgresql" % "postgresql" % "42.2.24"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", _*) => MergeStrategy.discard
 case _                        => MergeStrategy.first
}