package com.github.ybnt

import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.Encoders
import com.github.ybnt.DataProcessor.{Items, Logs, Ratings, Users, loadData, recommendation}

object DataPreProcessor {


  //make table
  case class recommend(UserID: Int, item1: Int, item2: Int, item3: Int, item4: Int)

  //need for calculate
  case class itemPair(Item1: Int, Item2: Int, rating1:Int, rating2:Int)
  case class itemPairsSimilarity(item1: Int, item2: Int, score: Double, numPairs: Long)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("DataPreProcessor")
      .setMaster("local[*]") // 로컬 모드에서 실행
      .set("spark.sql.broadcastTimeout","10000")
      .set("spark.shuffle.memoryFraction", "0.4")

    val spark = SparkSession
      .builder
      .appName("DataPreProcessor")
      .master("local[*]")
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    //define schema
    val LogsSchema = new StructType()
      .add("UserID", IntegerType, nullable = true)
      .add("Action", StringType, nullable = true)
      .add("AccessPath", StringType, nullable = true)
      .add("timestamp", LongType, nullable = true)
      .add("Rating", IntegerType, nullable = true)
      .add("ItemID", IntegerType, nullable = true)

    val UsersSchema = new StructType()
      .add("UserID", IntegerType, nullable = true)
      .add("Username", StringType, nullable = true)
      .add("Sex", BooleanType, nullable = true)
      .add("Address", StringType, nullable = true)
      .add("Mail", StringType, nullable = true)
      .add("BirthDate", LongType, nullable = true)

    val ItemsSchema = new StructType()
      .add("ItemID", IntegerType, nullable = true)
      .add("Name", StringType, nullable = true)
      .add("Price", FloatType, nullable = true)
      .add("Category", StringType, nullable = true)

    val RatingsSchema = new StructType()
      .add("UserID", IntegerType, nullable = true)
      .add("ItemID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    // read csv
    //    val logsData = spark.readStream.text("data/logs")
    val logsData = spark.read
      .option("header", "true")
      .schema(LogsSchema)
      .csv("data/log/log.csv")
      .as[Logs]

    val usersData = spark.read
      .option("header", "true")
      .schema(UsersSchema)
      .csv("data/usersdata.csv")
      .as[Users]

    val itemsData = spark.read
      .option("header", "true")
      .schema(ItemsSchema)
      .csv("data/itemsdata.csv")
      .as[Items]

    val ratingData = spark.read
      .option("sep", "\t")
      .schema(RatingsSchema)
      .csv("data/rating.dat")
      .as[Ratings]


    val ratings = ratingData.select("UserID", "ItemID", "rating")

    val recommendData = recommendation(spark,logsData,ratings)

    loadData(recommendData.toDF(),sys.env("PostgreSQL_URI"),"recommend",sys.env("airflow"),sys.env("airflow"))
    loadData(ratingData.toDF(),sys.env("PostgreSQL_URI"),"RatingData",sys.env("airflow"),sys.env("airflow"))
    loadData(itemsData.toDF(),sys.env("PostgreSQL_URI"),"ItemData",sys.env("airflow"),sys.env("airflow"))
    loadData(usersData.toDF(),sys.env("PostgreSQL_URI"),"UserData",sys.env("airflow"),sys.env("airflow"))

  }
}
