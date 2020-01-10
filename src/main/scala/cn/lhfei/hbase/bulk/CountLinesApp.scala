package cn.lhfei.hbase.bulk

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._
import org.apache.spark.sql.Row



object CountLinesApp extends App {
  import System._

  val src = Source.fromFile("D:\\Workspaces\\webapps_scala\\scala-hbase-app\\src\\data\\dmbout.txt")
  

//  val count = src.getLines().map(x => 1).sum
//
//  println(count)
  
  def extract(line: Row): Unit = {
    line.length
  }
  
  val spark = SparkSession.builder()
      .appName("Spark Dataframe example by Scala.")
      .getOrCreate();
  
  import spark.implicits._
  val df = spark.read.text("");
  
  df.collect().foreach(line => this.extract(line))
  
}