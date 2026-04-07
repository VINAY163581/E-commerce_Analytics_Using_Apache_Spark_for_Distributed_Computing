package com.analytics.data

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataLoader {

  private def readCsv(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .csv(path)

  def loadEvents(spark: SparkSession, path: String): DataFrame =
    readCsv(spark, path)
      .withColumn("timestamp", to_timestamp(col("timestamp")))

  def loadOrders(spark: SparkSession, path: String): DataFrame =
    readCsv(spark, path)
      .withColumn("timestamp", to_timestamp(col("timestamp")))

  def loadCatalog(spark: SparkSession, path: String): DataFrame =
    readCsv(spark, path)
}
