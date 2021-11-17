package com.nidisoft.scala
import org.apache.spark.rdd.RDD
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.rdd
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, size, split, stddev_pop, stddev_samp, sum, to_date, udf, when, year}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

import scala.collection.mutable


object elonmuskTweets {

  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)


   val spark = SparkSession
     .builder()
     .master("local")
     .appName("elonmuskTweets-example")
     .getOrCreate()

    import spark.implicits._


    val sqlContext = spark.sqlContext


    // elonmuskData: DataFrame
    val elonmuskData = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("src/main/elonmusk_tweets.csv")


    // display top 10 rows in elonmuskData dataframe
    println("Original DataFrame : ")
    elonmuskData.show(10)

    //val keyWords: List[String] = List("so", "the")

    println("PLEASE enter list of keywords separated by comma : ")
    val a=scala.io.StdIn.readLine()
    val keyWords : List[String] = a.split(",").map(_.trim).toList


   // select Date and Text from original df
    val elonmuskDataUpdate = elonmuskData.select(split(col("created_at")," ").getItem(0).as("Date") , 'text  )
    println("Select date and text from original dataframe")
    elonmuskDataUpdate.show(5)


    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    println("-------------------------------------------------------------------------------------------------")
    // udf that is calculate the distribution of keywords over time
    def CountNumOfWord(word: String) = udf((str: String) =>
      word.r.findAllIn(str).size
    )

    // another function that is added new column with the word occurrence
    def NumberWithWord(W : String)(df2: DataFrame): DataFrame = {
     df2.withColumn(colName = W, CountNumOfWord(W)($"text"))
    }

    // for each word in keywords list do this :
  keyWords.foreach(word=> {

    // calculate the number of word occurrence in each tweet by calling NumberWithWord udf
    val newDF = elonmuskDataUpdate.transform(NumberWithWord(word))
    // show the df
    println(word+" occurrence in each tweet : ")
    newDF.show(4)

    // then : group them by Date and calculate the sum of word occurrence in each day
    val newDF2 = newDF.groupBy("Date").sum(word)
    println("Dataframe after grouping with date and found the sum of "+word+" occurrence : ")

    //show df
    newDF2.show(5)

    // print the data as tupels
   val newRDD=newDF2.rdd
    newRDD
      .map(line => (word,line(0),line(1)))
      .foreach(println(_))

    })


    //System.exit(0)

    //////////////////////////////////////////////////////////////////////////////////////

    println("-------------------------------------------------------------------------------------------------")

    // udf that create an array of integer that contain 1 if word appear in the tweet 0 if not
    def countPercentage = udf((s: String) => {
      keyWords.map(i => if(i.r.findAllIn(s).size>=1) 1 else 0)
    })

    // add new column with the list from countPercentage function
    val resDF = elonmuskDataUpdate.withColumn("count", countPercentage(col("text")))

    // udf that found the sum of list
    def countSummation = udf((x: mutable.WrappedArray[Int]) => {
      x.sum
    })

    // add new coulmn with countSummation function
    val DFwithsummation = resDF.withColumn("summation",countSummation(col("count")))

    // show what is df look like now :
    println("df after added an array and sum of list :  ")
    DFwithsummation.show(3, false)


    //2- the percentage of tweets that have at least one of these input keywords.
    println("the percentage of tweets that have at least one of these input keywords : " ,(DFwithsummation.filter('summation > 0 ).count().toFloat/DFwithsummation.count().toFloat )*100)

    /////////////////////////////////////////////////////////////////////////////////////
    //3-the percentage of tweets that have at most two of the input keywords.
    println("the percentage of tweets that have at most two of the input keywords : " ,(DFwithsummation.filter('summation <= 2).count().toFloat/DFwithsummation.count().toFloat )*100)

    //////////////////////////////////////////////////////////////////////////////////////////////////////////


    println("-------------------------------------------------------------------------------------------------")
    //4- the average and standard deviation of the length of tweets

    val dfCounter = elonmuskDataUpdate.withColumn("size",size(split($"text" ," ")))
    println("DF with number of words in each tweet")
    dfCounter.show(10)
    println("the average and standard deviation of the length of tweets")
    dfCounter.select(avg('size)).show()
    dfCounter.agg(stddev_pop('size)).show()


    System.exit(0)
  }
}
