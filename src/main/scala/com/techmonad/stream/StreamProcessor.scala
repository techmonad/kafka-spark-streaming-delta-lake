package com.techmonad.stream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object StreamProcessor {

  def createFromKafka(topic: String, hosts: List[String])(implicit spark: SparkSession): DataFrame = {
    spark
      .readStream
      .option("kafka.bootstrap.servers", hosts.mkString(","))
      .option("subscribe", topic)
      .format("kafka")
      .load()
  }

  def process(stream: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val schema = Encoders.product[Message].schema
    stream
      .selectExpr("CAST(value as String)")
      .select(from_json($"value", schema = schema).as("message"))
      .selectExpr("CAST(message.time / 1000 as Timestamp) as time", "message.value")
      .withColumn("ts_trunc", date_trunc("HOUR", $"time"))
    //.select (($"time"/ 1000).cast(TimestampType).as("time"), $"value")
    //.withWatermark("time", "30 minutes")
    //  .groupBy(window($"time", "10 minutes", "5 minutes"), $"value")
    //.count()
  }

  def toDelta(df: DataFrame): StreamingQuery = {
    df
      .writeStream
      .format("delta")
      .option("checkpointLocation", "checkpoint-data")
      .partitionBy("ts_trunc") // partition data by hour
      .outputMode("append")
      .start("stream-data")
  }


}


case class Message(time: Long, value: Int)