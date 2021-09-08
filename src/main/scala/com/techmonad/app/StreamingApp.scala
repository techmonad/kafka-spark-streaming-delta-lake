package com.techmonad.app

import com.techmonad.stream.StreamProcessor.{createFromKafka, process, toDelta}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingApp {


  def main(args: Array[String]): Unit = {
    val hosts = List("localhost:9092")

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Kafka2DeltaStreaming")
        .getOrCreate()

    /**
     * Create stream from Kafka
     */
    val stream: DataFrame =createFromKafka( "event_data", hosts)

    /**
     * Apply transformations
     */
    val df = process(stream)

    // write to Delta
    val query = toDelta(df)

    query.awaitTermination()
    spark.stop()

  }

}
