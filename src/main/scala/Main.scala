package com.zinxon.spark

import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types._


object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.streaming.stopGracefullyOnShutDown", value = true)
      .config("spark.sql.shuffle.partitions", value = 3)
      .getOrCreate()

    val df: DataFrame = spark.read.option("header", value = true).option("inferSchema", value = true).csv("data/AAPL.csv")
    df.sort(col("date").desc).show()
    df.printSchema()

    val column = df("Open")
    val newColumn = (column + 2.0).as("OpenIncreasedBy2")
    val stringColumn = (column.cast(StringType)).as("OpenAsString")
    df.select(column, newColumn, stringColumn).filter(newColumn > 2.0).filter(newColumn > column).show()

    val renamedColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

    //              val stockData = df.select(renamedColumns: _*).withColumn("diff", col("close")-col("open")).filter(col("close")>col("open")*1.1)
    val stockData = df.select(renamedColumns: _*)
    stockData
      .groupBy(year(col("date")).as("year"))
      .agg(functions.max(col("close")).as("maxClose"), functions.avg(col("close")).as("avgClose"))
      .sort(col("maxClose").desc)
      .show()

    val highestClosingPrices = highestClosingPricesPerYear(stockData)
  }

  def highestClosingPricesPerYear(df: DataFrame): DataFrame = {
    val window = Window.partitionBy(year(col("date")).as("year"))
      .orderBy(col("close").desc)
    df
      .withColumn("rank", row_number().over(window))
      .filter(col("rank") === 1)
      .drop("rank")
      .sort(col("close").desc)
  }

  def add(x: Int, y: Int): Int = {
    return x + y - 1
  }
}





