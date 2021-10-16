package com.apple.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @Program: Default (Template) Project
  * @ClassName: GroupConcatDistinctUDAF
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-14 20:59
  * @Version 1.1.0
  **/

/**
  * group_concat_distinct(concat_long_string(city_id,city_name,':')    {"product_status": 1}
  *
  * product_status: 1
  */
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
  //inputSchema，指的是，输入数据的类型
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType) :: Nil)

  //bufferSchema，指的是，中间进行聚合时，所处理的数据类型
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType) :: Nil)

  //dataType，指的是，函数返回的类型
  override def dataType: DataType = StringType

  //聚合函数是否是幂等的，即相同输入是否总是能得到相同输出
  override def deterministic: Boolean = true

  // 初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  /**
    * 给聚合函数传入一条新数据进行处理
    * 更新
    * 可以认为是，一个一个地将组内的字段值传递进来
    * 实现拼接的逻辑
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //缓冲中已经拼过的城市信息串
    var bufferCityInfo = buffer.getString(0)
    val cityInfo = input.getString(0)
    if (!bufferCityInfo.contains(cityInfo)) {
      if ("".equals(bufferCityInfo)) {
        bufferCityInfo += cityInfo
      } else {
        // 比如1:北京
        // 1:北京,2:上海
        bufferCityInfo += "," + cityInfo
      }
      buffer.update(0, bufferCityInfo)
    }
  }

  // 合并聚合函数缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0);
    val bufferCityInfo2 = buffer2.getString(0);

    for (cityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo)) {
        if ("".equals(bufferCityInfo1)) {
          bufferCityInfo1 += cityInfo;
        } else {
          bufferCityInfo1 += "," + cityInfo;
        }
      }
    }
    buffer1.update(0, bufferCityInfo1);
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
