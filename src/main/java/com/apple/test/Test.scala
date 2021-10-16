package apple.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Program: commerce
  * @ClassName: Test
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-12 14:49
  * @Version 1.1.0
  **/
object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SessionAnalyzer").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    System.setProperty("HADOOP_USER_NAME", "ubuntu")
    System.setProperty("user", "ubuntu")
    sc.setLogLevel("ERROR")
    //    spark.sql(" DROP TABLE IF EXISTS user_info").show()
    //    spark.sql(
    //      """
    //        |
    //        | create table user_info(user_id Long,name string,age int,professional String,city String,sex String)
    //        | stored as textfile
    //        | row format delimited fields terminated by "\t"
    //        | LINES TERMINATED BY '\n'
    //        | LOCATION '/hive/data/user_info'
    //        |
    //      """.stripMargin).show()

    //spark.sql("""select * from user_visit_action""").show(20, false)

    //    spark.sql(
    //      """
    //        | select * from user_info
    //      """.stripMargin).show(20, false)


    val startDate = "2021-10-12"
    val endDate = "2021-10-14"

    val clickActionDF = spark.sql(
      s"""
         | select city_id,click_product_id,date from user_visit_action
         |  where click_product_id IS NOT NULL and click_product_id != -1L
         |  and date >='$startDate' and date<='$endDate'
      """.stripMargin).show(20, false)
  }
}
