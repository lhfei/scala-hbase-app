package cn.lhfei.hbase.bulk

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row

object SparkBulkLoader extends App {
  val spark = SparkSession.builder()
      .appName("Spark Dataframe example by Scala.")
      .getOrCreate();
  
  val df = spark.read.orc("/warehouse/tablespace/managed/hive/benchmark.db/odm_pay_merchant_account_details_x_i_d/dt=2020-03-14/000000_0_copy_15")

  df.createOrReplaceTempView("paybill")
  
  var lines = spark.sql("select * from paybill limit 5")
  
  def extract(items: Row) = {

  }
  
}