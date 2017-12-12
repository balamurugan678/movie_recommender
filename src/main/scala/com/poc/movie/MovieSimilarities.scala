package com.poc.movie

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }

import scala.io.{ Codec, Source }
import scala.math.sqrt

object MovieSimilarities {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("/Users/bgurus/balamurugan/Software/movie_recommender/datasets/movies.csv").getLines().drop(1)

    /**
      * UnComment this when a million movie dataset needs to be loaded
      */
    /*
    val lines = Source.fromFile("/Users/bgurus/balamurugan/Software/movie_recommender/datasets/movies.dat").getLines()
    for (line <- lines) {
      var internalFields = line.split("::")
      movieNames += (internalFields(0).toInt -> internalFields(1))
    }*/

    for (line <- lines) {
      var internalFields = line.split(',')
      movieNames += (internalFields(0).toInt -> internalFields(1))
    }

    return movieNames
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def makePairs(userRatings: UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    return movie1 < movie2
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx * sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .set("spark.driver.host", "localhost")
      .setAppName("MovieSimilarities")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    println("\nLoading movie names...")
    val nameDict = loadMovieNames()
    println("\nLoaded movie names..." + nameDict.size)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    val csvData = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bgurus/balamurugan/Software/movie_recommender/datasets/ratings.csv")

    /**
      * UnComment this when a million movie dataset needs to be loaded
      */
    //val rddData = sc.textFile("/Users/bgurus/balamurugan/Software/movie_recommender/datasets/ratings.dat")

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val rddData = csvData.rdd.map(x => x.mkString(","))

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings = rddData.map(l => l.split(",")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratings.join(ratings)

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))
    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()

    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")
    // Extract similarities for the movie we care about that are "good".

    if (args.length > 0) {
      val scoreThreshold = args(1).toDouble
      val coOccurenceThreshold = args(2).toDouble

      val movieID: Int = args(0).toInt
      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above
      val filteredResults = moviePairSimilarities.filter(x => {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
      })

      // Sort by quality score.
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(false).take(10)
      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }

    println("Movie recommended************")

    sc.stop()
  }
}