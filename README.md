# Spark RDD APIs
This project is focused on building a Spark application using Scala to extract statistics from a large number of tweets posted by Elon Musk stored in a CSV file. The application receives a list of keywords as input and calculates the following statistics:

## Getting Started
To get started with the project, you will need to have Spark and Scala installed on your machine. Additionally, you will need to import the necessary libraries and the csv file containing the tweets.

## Prerequisites
- [Apache Spark](https://spark.apache.org/)
- [Scala](https://www.scala-lang.org/)

## Installing
To install Spark and Scala, please follow the instructions provided on the respective websites.

## Running the Application
The application is run by inputting the keywords in a comma-separated list as a command-line argument.
For example, to run the application with keywords "Tesla" and "SpaceX", the command to run the application is as follows:

`SparkRDD.scala Tesla, SpaceX`
The application will then output the statistics as mentioned in the project description.

## Note
The application uses Spark RDD APIs to process the data and extract the necessary statistics. It's a hands-on project to master Scala and Spark for data processing and analysis.
