package solution

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Solution {
  val weeks = 8
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  lazy val spark: SparkSession =
    SparkSession.builder()
      .appName("solution")
      .master("local")
      .getOrCreate()

  import spark.implicits._

  def solve(now: DateTime, txns: DataFrame, catalog: DataFrame): DataFrame = {
    val joined = txns.join(catalog, txns("product_sku") === catalog("product_sku"))

    val filtered = joined
      .select(
        "customer_uuid",
        "date",
        "product_category",
        "usd_total",
        "date"
      ).where(withinDateRangeUDF(weeksBefore(now), now)(col("date")))

    val withFoodNotFood = filtered
      .withColumn("food", oneIfFood(filtered("product_category")))
      .withColumn("not_food", oneIfNotFood(filtered("product_category")))

    val grouped = withFoodNotFood
      .groupBy("customer_uuid")
      .agg(
        sum("food").alias("food_count"),
        sum("not_food").alias("not_food_count"),
        countDistinct("date").alias("total_visits"),
        sum(filtered("usd_total")).alias("total_spend")
      )

    val result = grouped
      .select(
        grouped("customer_uuid"),
        divideByWeeks(grouped("total_spend")).alias("weekly_spend"),
        divideByWeeksInt(grouped("food_count")).as("weekly_food_purchases"),
        divideByWeeksInt(grouped("not_food_count")).as("weekly_nonfood_purchases"),
        divideByWeeksInt(grouped("total_visits")).as("weekly_visits")
      )

    result
  }

  def main(args: Array[String]): Unit = {
    args.toList match {
      case List(catalogPath, txnsPath, outPath, date) =>
        val now = DateTime.parse(date, fmt)
        solve(now, readCSV(txnsPath), readCSV(catalogPath))
          .coalesce(1)
          .write.format("csv").option("header", true)
          .save(outPath)
      case _ =>
        println("\tusage: java -jar solution-assembly-0.1-SNAPSHOT.jar <catalog> <txns> <out> <yyyy-MM-dd>")
    }
  }

  val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def readCSV(file: String): DataFrame = {
    spark.read.format("csv").option("header", "true").load(file)
  }


  def weeksBefore(dt: DateTime) =
    dt.minusWeeks(weeks)

  def calendarDateUDF =
    udf((d: String) => DateTime.parse(d, fmt).toLocalDate().toString)

  def oneIfFood: UserDefinedFunction =
    udf((d: String) => {
      if (d == "Food") 1 else 0
    })

  def oneIfNotFood: UserDefinedFunction =
    udf((d: String) => {
      if (d != "Food") 1 else 0
    })

  def divideByWeeks: UserDefinedFunction =
    udf((d: Double) => {
      d / weeks
    })

  def divideByWeeksInt: UserDefinedFunction =
    udf((i: Int) => {
      i.toDouble / weeks
    })

  def withinDateRangeUDF(startDate: DateTime, endDate: DateTime) =
    udf((d: String) => {
          val dt = DateTime.parse(d, fmt)
          dt.isAfter(startDate) && dt.isBefore(endDate)
        })
}
