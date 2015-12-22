package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))

    // read the file
    val file = sc.textFile("hdfs://localhost:8020" + args(0))
 
    // split each document into words
	val split_lines = file.map(_.split("\t"))
  	
    val tsvmap = split_lines.map(col => col(1) -> col(2).toInt)
    val incomingNodes = tsvmap.reduceByKey(_ + _).sortByKey(true)
   
    // store output on given HDFS path.
	val incomingDeg = incomingNodes.map{ case (x,y) => Array(x, y).mkString("\t") }
    incomingDeg.saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}
