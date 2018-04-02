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
import org.apache.spark.sql.functions.collect_list





object data1 {
  val sc = new SparkContext(new SparkConf().setAppName("Fragma Data sets2"))
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
   case class Users(UserID: Int, Gender: String,Age: String, Occupation: String, ZipCode: String)
   object User{                                                            //Parse Function for parsing Movies
        def parse(line:String): Users={
                      val rows = line.split("::")
                      Users(rows(0).toInt,rows(1).toString,rows(2).toString,rows(3).toString,rows(4).toString)
                      //Age and occupation taken as string for easy substituiton later
        }
      }
   
  
      def main(args:Array[String]){
            
        //Intialisation of Key value pairs Map for Occupation and Age
            val OccMap = Map(("0","other"),("2","artist"),("3","clerical/admin"),("4","college/grad student"),("5","customer service"),("6","doctor/health care"),("1","academic/educator"),("7","executive/managerial"),("8","farmer"),("9","homemaker"),("10","K-12 student"),("11","lawyer"),("12","programmer"),("13","retired"),("14","sales/marketing"),("15","scientist"),("16","self-employed"),("17","technician/engineer"),("18","tradesman/craftsman"),("19","unemployed"),("20","writer"))
            val AgeMap= Map(("18","18-35"),("25","18-35"),("35","36-50"),("45","36-50"),("50","50+"),("56","50+"))
        //Loading Of files args(0)= Rating file , args(1)= Movie File args(2)= User File   
            val ratingsFile = sc.textFile(args(0)).map(Rating.parse)
            val moviesFile = sc.textFile(args(1)).map(Movie.parse)  
            val usersFile = sc.textFile(args(2)).map(User.parse)
            
          //Convesrion to Dataframes
            val ratingsDF = ratingsFile.toDF
            val moviesDF = moviesFile.toDF
            val usersDF = usersFile.toDF
          
          //Spliting of Genre column into array and exploding values of genres 
            val moviesDF1 = moviesDF.withColumn("Genres", split(moviesDF("Genres"), "\\|").cast("array<String>")).withColumn("Genres", explode($"Genres"))
           
          //Filtering "under 18" and replacing values for HashMaps
            val usersDF1= usersDF.filter(usersDF("Age") !== "1").na.replace("Age",AgeMap).na.replace("Occupation",OccMap)
          //Joining of Dataframes
            val joined_df = moviesDF1.join(ratingsDF, "MovieID")
            val joined_df2= joined_df.join(usersDF1, "UserID")
           //Grouping by using aggregation as Average Rating
            val avergaeDF = joined_df2.groupBy(col("Age"),col("Occupation"),col("Genres")).avg("Rating")         
           //Creating a udf for merging two columns
            val combine = udf[Seq[(Double, String)], Seq[Double], Seq[String]](_.zip(_))
           // Collect values of all generes with their respective rating  as Array of Structs for each combination of Age and Occupation
            val OcAgDF = avergaeDF.groupBy(col("Occupation"),col("Age")).agg(collect_list(col("Genres")) as "Genres",collect_list(col("avg(Rating)")) as "rating").withColumn("Genres", combine(col("rating"), col("Genres"))).drop("rating")
           //Sorting of Array of Structs based on rating
            val sortedDF=OcAgDF.withColumn("Sorted",sort_array(col("Genres"),false)).drop("Genres")
            
           // Flattening of array of structs into 5 columns - Rank 1 to Rank 5
            val finalDF =sortedDF.withColumn("Rank1", $"Sorted._2"(0)).withColumn("Rank2", $"Sorted._2"(1)).withColumn("Rank3", $"Sorted._2"(2)).withColumn("Rank4", $"Sorted._2"(3)).withColumn("Rank5", $"Sorted._2"(4)).drop("Sorted")
  
             print("Genre Ranking by Average Rating:\n")
            
             finalDF.show(300,false)
            
            
            //uncomment to save data in table and print full output using hive for larger datasets
            //finalDF.collect().foreach(println)
            //finalDF.registerTempTable("FinalTable")
            //sqlContext.sql("Create Database.Table AS FinalTable;")
            //sqlContext.sql("Select * from Database.Table;")
            
            
            
            

             

      
      }
      
 

   
      
}