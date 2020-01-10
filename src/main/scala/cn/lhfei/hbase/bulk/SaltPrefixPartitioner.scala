package cn.lhfei.hbase.bulk

import org.apache.spark.Partitioner

//class SaltPrefixPartitioner[K,V](modulus: Int) extends Partitioner {
//  val charsInSalt = digitsRequired(modulus)
//
//  def getPartition(key: String): Int = {
//    key.substring(0,charsInSalt).toInt
//  }
//
//  override def numPartitions: Int = modulus
//}