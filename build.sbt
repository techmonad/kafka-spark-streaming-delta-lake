name := "kafka-to-delta-lake-by-spark-streaming"

version := "1.0"

scalaVersion := "2.12.10"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2",
  "io.delta" %% "delta-core" % "1.0.0"
)
