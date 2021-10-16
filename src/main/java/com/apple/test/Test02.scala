package com.apple.test

import org.apache.spark.sql.SparkSession
;

/**
  * @Program: commerceDemo
  * @ClassName: Test02
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-13 17:56
  * @Version 1.1.0
  **/
object Test02 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Test02")
      .master("local[*]").getOrCreate()
    val rdd =
      sparkSession
        .sparkContext
        .textFile("data\\a.txt")
    val b = rdd.map { line =>
      val strArr = line.split("\t")(1).split("\\|")
      for (str <- strArr) {
        val strrArr = str.split("=")
        if (strrArr(0) == "age") {
          println(strrArr(0) + "::" + strrArr(1))
        }
      }
    }
  }
}
