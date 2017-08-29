##Related tags interview question
================================================

A tag is a label provided by the reader of the given document. A document can have one or more tags. 
We would like to know the tags which are related to eachother.  Tags are related to eachother if they occur multiple times together.

Build
=====
$ mvn clean install -DskipTests

Run
====
args(0) = maximumTagsPerTag (1 or 2)
args(1) = outputfile name
$ spark-submit --class com.ali.interview.RelatedTags ./target/spark-interview-offline.jar 1 /tmp/relatedTags
  

Solution
========
A partial solution has been implemented for this coding task.  

Two type of output:
1. maximumTagsPerTag: 1 -> which output result very similar to bigrams in a document. However the last elemnt does not pair with fisrt element.

  def bigramsInString(s: String): Array[(String, String)] = { 
    s.split("""\.""")                 // split on .
    .map(_.split(",")                 // split on comma
    .filter(_.nonEmpty)               // remove empty string
    .map(_.replaceAll("""\W""", ""))  // remove special chars
    .filter(_.nonEmpty)                
    .sliding(2)                       // take continuous pairs
    .filter(_.size == 2)              // sliding can return partial
    .map{ case Array(a, b) => (a, b) })
    .flatMap(x => x)                         
  }

The method above gives good result however it does not include the pair for last elemnt and the fisrt element 

2.  maximumTagsPerTag: 2 -> need to choose the highest occurrence tag number, and than second highest occurence number. This was implemented 
using following code snipet:

val relatedTags = data.flatMap(line=> {
  val tags = line.split(",").map(_.trim)
  val doubleSeq = for(i <- 0 until tags.length) yield {
    val x = tags(i)
    for(j <- (i+1) until tags.length) yield {
      val y = tags(j)
      (x,y)
    }
   }
   doubleSeq.flatten
})	
 
Both were tested using sprk-shell
==================================
scala> val data = sc.textFile("./spark-interview-offline/src/main/resources/tags.csv")

scala>   def bigramsInString(s: String): Array[(String, String)] = { 
     |     s.split("""\.""")                 // split on .
     |     .map(_.split(",")                 // split on comma
     |     .filter(_.nonEmpty)               // remove empty string
     |     .map(_.replaceAll("""\W""", ""))  // remove special chars
     |     .filter(_.nonEmpty)                
     |     .sliding(2)                       // take continuous pairs
     |     .filter(_.size == 2)              // sliding can return partial
     |     .map{ case Array(a, b) => (a, b) })
     |     .flatMap(x => x)                         
     |   }
bigramsInString: (s: String)Array[(String, String)]

scala> val relatedTags=data.map(bigramsInString).flatMap(x => x) 
relatedTags: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[154] at flatMap at <console>:30

scala> relatedTags.collect
res218: Array[(String, String)] = Array((A,B), (B,C), (A,C), (apple,lemon), (A,lemon))

scala> relatedTags.saveAsTextFile("/tmp/relatedTags")

Check the output at /tmp/relatedTags
more /tmp/relatedTags/part-00000
(A,B)
(B,C)
(A,C)
(apple,lemon)

Testing maximumTagsPerTag: 2
scala> val relatedTags = data.flatMap(line=>{
     | val arr = line.split(",").map(_.trim)
     |  val doubleSeq = for(i <- 0 until arr.length) yield {
     |   val x = arr(i)
     |   for(j <- (i+1) until arr.length) yield {
     |    val y = arr(j)
     |    (x,y)
     |   }
     |  }
     |  doubleSeq.flatten
     | })
relatedTags: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[156] at flatMap at <console>:28

scala> relatedTags.collect
res222: Array[(String, String)] = Array((apple,fruit), (apple,food), (apple,peach), (fruit,food), (fruit,peach), (food,peach), (house,building))

Alternative solution
====================
It seems DataFrame could be used for solving these problem using Spark SQL.

scala> val relatedTagsDF = data.toDF("tags")
relatedTagsDF: org.apache.spark.sql.DataFrame = [tags: string]
 
scala> relatedTagsDF.show()
+------------+
|        tags|
+------------+
|        tags|
|       A,B,C|
|         A,C|
|apple, lemon|
|       apple|
|    A, lemon|
+------------+

// Register the DataFrame as a SQL temporary view
scala> relatedTagsDF.createOrReplaceTempView("tags")

scala> val sqlDF = spark.sql("SELECT * FROM tags")
sqlDF: org.apache.spark.sql.DataFrame = [tags: string]

scala> sqlDF.show()
+------------+
|        tags|
+------------+
|        tags|
|       A,B,C|
|         A,C|
|apple, lemon|
|       apple|
|    A, lemon|
+------------+

TODO
====
Complete the solution
Write test classes for unit testing


