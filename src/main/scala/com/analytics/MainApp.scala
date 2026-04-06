package com.analytics

import com.analytics.data.DataLoader
import com.analytics.session.Sessionization
import com.analytics.funnel.FunnelAnalysis
import com.analytics.attribution.Attribution
import com.analytics.anomaly.AnomalyDetection
import com.analytics.metrics.Metrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.Instant
import java.sql.Timestamp

object MainApp {

  private def writeToBigQuery(
      df: DataFrame,
      projectId: String,
      dataset: String,
      table: String,
      temporaryBucket: String,
      mode: String
  ): Unit = {
    df.write
      .format("bigquery")
      .option("table", s"$projectId:$dataset.$table")
      .option("temporaryGcsBucket", temporaryBucket)
      .mode(mode)
      .save()
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Ecommerce Analytics")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Use DATA_PATH if provided (recommended in Dataproc submit),
    // otherwise fall back to your real bucket path.
    val basePath = sys.env.getOrElse("DATA_PATH", "gs://vinay-ecommerce-dataproc-6731/data")

    println(s"Using DATA_PATH = $basePath")
    println(s"Loading files:")
    println(s"  events  -> $basePath/events.csv")
    println(s"  orders  -> $basePath/orders.csv")
    println(s"  catalog -> $basePath/catalog.csv")

    val events  = DataLoader.loadEvents(spark, s"$basePath/events.csv")
    val orders  = DataLoader.loadOrders(spark, s"$basePath/orders.csv")
    val catalog = DataLoader.loadCatalog(spark, s"$basePath/catalog.csv")

    val sessions = Sessionization.buildSessions(events)

    val funnel = FunnelAnalysis.computeFunnel(sessions, catalog)

    val lookbackHours = 24
    val attribution = Attribution.lastTouchAttribution(
      sessions,
      orders,
      lookbackHours
    )

    val anomalies = AnomalyDetection.detectAnomalies(funnel)

    val totalEvents = Metrics.totalEvents(events)
    val totalOrders = Metrics.totalOrders(orders)
    val totalRevenue = Metrics.totalRevenue(orders)
    val averageOrderValue = Metrics.averageOrderValue(orders)
    val conversionRate = Metrics.conversionRate(events, orders)
    val eventsByType = Metrics.eventsByType(events)
    val topSellingProducts = Metrics.topSellingProducts(orders, catalog)

    println("========= METRICS =========")
    println(s"Total Events: $totalEvents")
    println(s"Total Orders: $totalOrders")
    println(s"Total Revenue: $totalRevenue")
    println(s"Average Order Value: $averageOrderValue")
    println(s"Conversion Rate: $conversionRate")

    println("\nEvents by Type:")
    eventsByType.show(false)

    println("\nTop Selling Products:")
    topSellingProducts.show(false)

    println("\n========= FUNNEL =========")
    funnel.show(false)

    println("\n========= ATTRIBUTION =========")
    attribution.show(false)

    println("\n========= ANOMALIES =========")
    anomalies.show(false)

    val bqProject = sys.env.getOrElse("BQ_PROJECT_ID", "project-34c13448-074a-417a-b64")
    val bqDataset = sys.env.getOrElse("BQ_DATASET", "ecommerce_analytics")
    val bqTablePrefix = sys.env.getOrElse("BQ_TABLE_PREFIX", "ecommerce")
    val tempBucket = sys.env.getOrElse("TEMP_GCS_BUCKET", "vinay-ecommerce-dataproc-6731")
    val runTs = Instant.now()

    import spark.implicits._
    val kpiSnapshot = Seq(
      (
        runTs.toString,
        Timestamp.from(runTs),
        totalEvents,
        totalOrders,
        totalRevenue,
        averageOrderValue,
        conversionRate
      )
    ).toDF(
      "run_id",
      "run_timestamp",
      "total_events",
      "total_orders",
      "total_revenue",
      "average_order_value",
      "conversion_rate"
    )

    println("\n========= WRITING RESULTS TO BIGQUERY =========")
    writeToBigQuery(kpiSnapshot, bqProject, bqDataset, s"${bqTablePrefix}_kpi_snapshot", tempBucket, "append")
    writeToBigQuery(eventsByType, bqProject, bqDataset, s"${bqTablePrefix}_events_by_type", tempBucket, "overwrite")
    writeToBigQuery(topSellingProducts, bqProject, bqDataset, s"${bqTablePrefix}_top_products", tempBucket, "overwrite")
    writeToBigQuery(funnel, bqProject, bqDataset, s"${bqTablePrefix}_funnel", tempBucket, "overwrite")
    writeToBigQuery(attribution, bqProject, bqDataset, s"${bqTablePrefix}_attribution", tempBucket, "overwrite")
    writeToBigQuery(anomalies, bqProject, bqDataset, s"${bqTablePrefix}_anomalies", tempBucket, "overwrite")
    println(s"BigQuery export complete: $bqProject:$bqDataset (${bqTablePrefix}_*)")

    spark.stop()
  }
}
