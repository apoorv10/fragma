package com.fragma

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, TimestampType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._


object data {
  val sc = new SparkContext(new SparkConf().setAppName("Fragma Data sets"))
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
   
  case class Ratings(UserID: Int, MovieID: Int, Rating: Int, TimeStamp: String)  //Schema For Ratings.dat
      
      object Rating{                                                            //Parse Function for parsing Ratings
        def parse(line:String): Ratings={
                      val rows = line.split("::")
                      Ratings(rows(0).toInt,rows(1).toInt,rows(2).toInt,rows(3).toString)
        }
      }
      
  case class Movies(MovieID: Int, Title: String, Genres: String)               //Schema For Movies.dat
      object Movie{                                                            //Parse Function for parsing Movies
        def parse(line:String): Movies={
                      val rows = line.split("::")
                      Movies(rows(0).toInt,rows(1).toString,rows(2).toString)
        }
      }
      
      
  
      def main(args:Array[String]){
            
            val ratingsFile = sc.textFile(args(0)).map(Rating.parse)          //File path for rating as 1st argument
            val ratingsDF = ratingsFile.toDF()
            
            val moviesFile = sc.textFile(args(1)).map(Movie.parse)           //File path for movies as 2nd argument
            val moviesDF = moviesFile.toDF()
           
            val viewCount = ratingsDF.groupBy("MovieID").agg(avg("Rating").alias("Rating"),count("MovieID").alias("count"))
            
            val topViews = viewCount.orderBy(desc("count")).limit(10).cache()   //top 10 watched movies and cahced in memory for faster joining
            
            val joinedDF = topViews.join(moviesDF, "MovieID")         
            //val joined_df = g.join(k, g.col("MovieID") === k.col("MovieID"), "inner").alias("Movie")
            val top10 = joinedDF.orderBy(desc("count")).select(col("MovieID"),col("Title"))
            print("Top 10 Most Viewed Movies(Top to bottom):\n")
            top10.show(false)

            topViews.unpersist()
            
            val above40 = viewCount.filter(viewCount("count") >= 40)
            val topRated = above40.orderBy(desc("Rating")).limit(20).cache()
            val joinedDF1 = topRated.join(moviesDF, "MovieID")
            val top20 = joinedDF1.orderBy(desc("Rating")).select(col("MovieID"),col("Title"),col("Rating"))
            print("Top 20 Highest Rated Movies(Top to bottom):\n")
            top20.show(false)

            topRated.unpersist()

         
      }
            
}