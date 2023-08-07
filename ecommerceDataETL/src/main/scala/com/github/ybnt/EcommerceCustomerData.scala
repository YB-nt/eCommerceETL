package com.github.ybnt

import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkConf


object EcommerceCustomerData {
  // defined schema
  case class Logs(UserID: Int, Action: String, AccessPath: String, TimeStamp: Long, Rating: Int, ItemID: Int)
  case class Users(UserID: Int, Username: String, Sex: Boolean, Address: String, Mail: String, BirthDate: Long)
  case class Items(ItemID: Int, Name: String, Price: Float, Category: String)
  case class Ratings(UserID: Int, ItemID: Int, rating: Int, timestamp: Long)

  //make table
  case class recommend(UserID: Int, item1: Int, item2: Int, item3: Int, item4: Int, item5: Int)

  //need for calculate
  case class itemPair(Item1: Int, Item2: Int, rating1:Int, rating2:Int)
  case class itemPairsSimilarity(item1: Int, item2: Int, score: Double, numPairs: Long)


  def computeCosineSimilarityWithWeight(spark: SparkSession, logs: Dataset[Logs], data: Dataset[itemPair]): Dataset[itemPairsSimilarity] = {
    // Logs 데이터셋에서 가중치를 부여하여 rating 값을 계산
    import spark.implicits._



    // itemPair 데이터셋과 weightedRatings 데이터셋을 조인하여 rating1과 rating2 값을 가져옴
//    val pairRatings = data
//      .join(weightedRatings.as("r1"), $"Item1" === $"r1.ItemID")
//      .join(weightedRatings.as("r2"), data.col("Item2") === $"r2.ItemID")
//      .select(col("Item1"), col("Item2"), col("r1.weightedRating").alias("rating1"), col("r2.weightedRating").alias("rating2"))

    // Compute xx, xy and yy columns
    val pairScores = data
      .withColumn("xx", col("rating1") * col("rating1"))
      .withColumn("yy", col("rating2") * col("rating2"))
      .withColumn("xy", col("rating1") * col("rating2"))
    pairScores.show()
    val calculateSimilarity = pairScores
      .groupBy("item1", "item2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )

    calculateSimilarity.show()
    val weightedRatings = logs.withColumn("weight", udf(weightUserAction _).apply(col("Action")))
      .groupBy("UserID", "ItemID")
      .agg(round(sum(col("Rating") * col("weight")),2).alias("weightedRating"))

    val result: Dataset[itemPairsSimilarity] = calculateSimilarity
      .withColumn("score",
        when(col("denominator") =!= 0, col("numerator") / col("denominator"))
          .otherwise(null)
      ).select("item1", "item2", "score", "numPairs").as[itemPairsSimilarity]

    result
  }

  //action 별로 가중치 정의
  def weightUserAction(Action: String): Double = {
    if (Action == "ItemSearch") {
      0.6
    }
    else if (Action == "Buy") {
      1.0
    }
    else if (Action == "AddtoCart") {
      0.8
    }
    else {
      0.2
    }
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("MyApp")
      .setMaster("local[*]") // 로컬 모드에서 실행
      .set("spark.shuffle.memoryFraction", "0.4")
      .set("spark.driver.bindAddress", "127.0.0.1")

    val spark = SparkSession
      .builder
      .appName("EcommerceCustomerData")
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

    //디렉토리 설정
    val checkpointDirectory = "./checkpoints"
    val outputDirectory = "./output"

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
      .option("header", "true")
      .schema(RatingsSchema)
      .csv("data/rating.csv")
      .as[Ratings]


    val ratings = ratingData.select("UserID", "ItemID", "rating")

    val itemPair = ratings.as("ratings1")
      .join(ratings.as("ratings2"), $"ratings1.UserID" === $"ratings2.UserID" && $"ratings1.ItemID" < $"ratings2.ItemID")
      .select($"ratings1.ItemID".alias("item1"),
        $"ratings2.ItemID".alias("item2"),
        $"ratings1.rating".alias("rating1"),
        $"ratings2.rating".alias("rating2")
      ).repartition(2000).as[itemPair]
//    itemPair.show(100)
    val itemPairsSimilarity = computeCosineSimilarityWithWeight(spark,logsData, itemPair).cache()
//    itemPairsSimilarity.show()
//    val scoreThreshold = 0.96
//    val coOccurrenceThreshold = 1000.0
//
//    // Threshold 값을 적용
//    val filteredResults = itemPairsSimilarity.filter(col("score") > scoreThreshold && col("numPairs") > coOccurrenceThreshold)
    // 데이터가 너무 적어서 값이 안잡힘 높은 평점의 데이터 많이 필요
    // 데이터 가중치를 통해서 높은 점수의 평가를 많이 생성 필요
//    filteredResults.show()
//    // 각 사용자가 평가한 상품 목룍
//    val userRatings = ratings.groupBy("UserID").agg(collect_set("ItemID").alias("reatedItems"))
////
////    // top5 뽑기
//    val topItems = filteredResults.orderBy(desc("Score"))
//      .groupBy("item1")
//      .agg(collect_list("item2").alias("similarItems"))
//      .select($"item1".as("Target_Item"),
//        when(size($"similarItems") >= 1, $"similarItems".getItem(0)).alias("item2"),
//        when(size($"similarItems") >= 2, $"similarItems".getItem(1)).alias("item3"),
//        when(size($"similarItems") >= 3, $"similarItems".getItem(2)).alias("item4"),
//        when(size($"similarItems") >= 4, $"similarItems".getItem(3)).alias("item5")
//      )

//    topItems.show()
//    val = recommendations = userRatings.join(topItems,$)


//    // top 5 상품 추천 결과를 recommend 데이터셋으로 변환
//    recommendations.as[recommend]

  }
}
