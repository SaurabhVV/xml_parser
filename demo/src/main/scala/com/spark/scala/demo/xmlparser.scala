package com.spark.scala.demo

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._

object xmlparser {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder() // Create SparkSession
      .master("local[1]")
      .appName("xmlparser")
      .getOrCreate()

    val xmldf = spark.read // Read XML file using Databricks XML library
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .xml("books_withnested_array.xml")

    xmldf.printSchema() // Printing Schema

    //    val datatypes = xmldf.dtypes.toMap
    //
    //    for (i <- datatypes) {
    //      println(i)
    //    }
    //
    //    if (datatypes("stores").contains("store,ArrayType")) {
    //      println("ArrayType")
    //    } else if (datatypes("stores").contains("store,StructType")) {
    //      println("StructType")
    //    }

    //    println(xmldf.schema)
    //    xmldf.drop("description").show(false)
    //    println(xmldf.count())

    xmldf.select("_id", "author", "genre", "price", "publish_date", "title").show(false) // Showing Non-Nested Columns (XML tags)

    xmldf.select("_id", "otherInfo.address", "otherInfo.country", "otherInfo.language", "otherInfo.pagesCount") // Showing StructType Nested Columns
      .select("_id", "address.addressline1", "address.city", "address.state", "country", "language", "pagesCount").show(false)

    xmldf.select("_id", "stores.store").withColumn("store", explode(col("store"))) // Showing ArrayType Nested Columns
      .select("_id", "store.location", "store.name").show(false)

  }
}
