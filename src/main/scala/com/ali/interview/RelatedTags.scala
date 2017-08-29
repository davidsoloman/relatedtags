package com.ali.interview
/**
 * RelatedTags - Tags are related to each other if they occur multiple times together.
*/

import org.apache.spark._
import org.apache.spark.SparkContext._

object RelatedTags {
  def main(args: Array[String]) {
    val maximumTagsPerTag = args(0).toInt
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("RelatedTags")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load input data.
    val data = sc.textFile("./src/main/resources/tags.csv")
    val relatedTags = {
	  if (maximumTagsPerTag > 1) {
		data.flatMap(line=>{
		  val tags = line.split(",").map(_.trim)
		  val doubleSeq = for(i <- 0 until tags.length) yield {
		  val x = tags(i)
		  for(j <- (i+1) until tags.length) yield {
			val y = tags(j)
			(x,y)
		  }
        }
        doubleSeq.flatten})
	  } else {
	    data.map(bigramsInString).flatMap(x => x)
	  }
	}
    // Save output to a text file
    relatedTags.saveAsTextFile(outputFile)
  }
  
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

}
