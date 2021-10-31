package com.nidisoft.scala
import org.apache.spark.rdd.RDD
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{ DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col ,count, size, split, stddev_pop, stddev_samp, sum, to_date, udf, when, year}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._


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


    //filter coulmn :
    val elonmuskDataFiltered = elonmuskData.withColumn("textNew", functions.regexp_replace(elonmuskData.col("text"), """[+._,'@ "]+""", " "))
    val elonmuskDataUpdate = elonmuskDataFiltered.select(split(col("created_at")," ").getItem(0).as("Date") , 'text  )
    elonmuskDataUpdate.show(5)

    //  System.exit(0)

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    println("-------------------------------------------------------------------------------------------------")
    // 1- the distribution of keywords over time
    def countAll(pattern: String) = udf((s: String) => pattern.r.findAllIn(s).size)
    def NumberWithWord(W : String)(df2: DataFrame): DataFrame = {
     df2.withColumn(colName = W, countAll(W)($"text"))
    }

  keyWords.foreach(word=> {
    val newDF = elonmuskDataUpdate.transform(NumberWithWord(word))
    println("Dataframe with new column : ")
    val newDF2 = newDF.groupBy("Date").sum(word)
    newDF2.show(5)

   val newRDD=newDF2.rdd
    newRDD
      .map(line => (word,line(0),line(1)))
      .foreach(println(_))
    })


    //System.exit(0)

    //////////////////////////////////////////////////////////////////////////////////////
    //2- the percentage of tweets that have at least one of these input keywords.
    println("-------------------------------------------------------------------------------------------------")

    val countPercentage = udf((s: String) => {
      keyWords.map(i => if(i.r.findAllIn(s).size>=1) 1 else 0)
    })

    val resDF = elonmuskDataUpdate.withColumn("count", countPercentage(col("text")))
    //var newresDF = resDF
   var newresDF = resDF
    for( i <- 1 to keyWords.length-1) {
      newresDF = resDF.withColumn("totalcount", resDF("count")(0)+resDF("count")(i))
  }

    newresDF.show(10)
    println("the percentage of tweets that have at least one of these input keywords : " ,(newresDF.filter('totalcount > 0 ).count().toFloat/newresDF.count().toFloat )*100)

    /////////////////////////////////////////////////////////////////////////////////////
    //3-the percentage of tweets that have at most two of the input keywords.

    println("the percentage of tweets that have at least one of these input keywords : " ,(newresDF.filter('totalcount <= 2).count().toFloat/newresDF.count().toFloat )*100)

    //////////////////////////////////////////////////////////////////////////////////////////////////////////

    //4- the average and standard deviation of the length of tweets

    println("-------------------------------------------------------------------------------------------------")

    val dfCounter = elonmuskDataUpdate.withColumn("size",size(split($"text" ," ")))
    dfCounter.show(10)
    println("the average and standard deviation of the length of tweets")
    dfCounter.select(avg('size)).show()
    dfCounter.agg(stddev_pop('size)).show()


    System.exit(0)
  }
}




