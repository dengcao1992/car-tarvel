package com.cartravel.spark.programApp

import com.cartravel.spark.SparkEngine
import com.cartravel.utils.GlobalConfigUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by angel
  */
object ReadApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = SparkEngine.getSparkConf()
    val session = SparkEngine.getSparkSession(sparkConf)
    //加载订单表
    var order:DataFrame = session.read
      .format(GlobalConfigUtils.getProp("custom.hbase.path"))
      .options(
        Map(
          GlobalConfigUtils.getProp("sparksql_table_schema") -> GlobalConfigUtils.getProp("order.sparksql_table_schema"),
          GlobalConfigUtils.getProp("hbase_table_name") -> GlobalConfigUtils.getProp("syn.table.order_info"),
          GlobalConfigUtils.getProp("hbase_table_schema") -> GlobalConfigUtils.getProp("order.hbase_table_schema")
        )).load()

    order.show()
  }
}
