package com.apple.test

import org.apache.spark.sql.SparkSession

/**
  * @Program: commerceDemo
  * @ClassName: Test03
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-13 18:27
  * @Version 1.1.0
  **/
object Test03 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Test02")
      .master("local[*]").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val rdd =
      sparkSession
        .sparkContext
        .textFile("data\\a.txt")
    val rdd2 = rdd.map { line =>
      val strArr = line.split("\t")(1)

      println(strArr)
    }.collect()
  }
}
