package com.zinxon.spark

import org.apache.spark.sql
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date
import java.util.Base64.Encoder

class FirstTest extends AnyFunSuite{
  private val spark = SparkSession
    .builder()
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()

  private val schema = StructType(
    Seq(
      StructField("date",DateType,nullable = true),
      StructField("open",DoubleType,nullable = true),
      StructField("close",DoubleType,nullable = true)
    )
  )

  test("add(2,3) returns 5"){
    val result = Main.add(2,3)
    assert(result==5)
  }

  test("Test highest Closing Prices Per Year"){
    val testRows = Seq(
      Row(Date.valueOf("2022-01-12"),1.0,2.0),
      Row(Date.valueOf("2023-03-01"),1.0,2.0),
      Row(Date.valueOf("2023-01-12"),1.0,3.0)
    )

    val expected = Seq(
      Row(Date.valueOf("2022-01-12"),1.0,2.0),
      Row(Date.valueOf("2023-01-12"),1.0,3.0)
    )
    implicit val encoder: sql.Encoder[Row] = Encoders.row(schema)
    val testDataFrame = spark.createDataset(testRows)
    val actualRows =  Main.highestClosingPricesPerYear(testDataFrame).collect()

//    actualRows should contain allElementsOf(expected)
    actualRows should contain theSameElementsAs expected
  }

}
