# BigData - Spark RDD APIs
we have a csv file containing a large number of tweets posted by Elon Musk, we need to build a spark application using scala, which extracts statistics about some entered keywords.  


 - The application receives a comma-separated list of keywords and prints the following for each keyword:  the distribution of keywords over time (day-wise), i.e., the number of times each keyword is mentioned every day. For example, the output something like that: (k1, 2-3-2021, 34), (k1, 3-3-2021, 14)
 -  the percentage of tweets that have at least one of these input keywords. 
 -  the percentage of tweets that have at most two of the input keywords. 
 -  the average and standard deviation of the length of tweets. Note that the length of tweet refers to the number of words in that tweet.
