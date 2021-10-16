package com.apple.commons.conf


import com.apple.commons.constant.Constants
import net.sf.json.JSONObject
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}

/**
  * @Program: commerce
  * @ClassName: ConfigurationManager
  * @Description: 工具类
  * @Author Mr.Apple
  * @Create: 2021-10-12 14:49
  * @Version 1.1.0
  *          https://blog.csdn.net/smallnetvisitor/article/details/81327131
  **/
object ConfigurationManager {

  // 通过getConfiguration获取配置对象
  val config = builder
  private val params = new Parameters()
  /**
    * FileBasedConfigurationBuilder:产生一个传入的类的实例对象
    * FileBasedConfiguration:融合FileBased与Configuration的接口
    * PropertiesConfiguration:从一个或者多个文件读取配置的标准配置加载器
    * configure():通过params实例初始化配置生成器
    * 向FileBasedConfigurationBuilder()中传入一个标准配置加载器类，
    * 生成一个加载器类的实例对象，然后通过params参数对其初始化
    */
  private val builder =
    new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
      .configure(params.properties().setFileName("commerce.properties")).getConfiguration

  // 创建用于初始化配置生成器实例的参数对象
  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)
    println("taskParam" + taskParam)
  }
}
