package com.cartravel.hbase.com.cartravel.hbase.org.custom.spark.sql.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

/**
  * Created by angel
  */
class DefaultSource extends RelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    HbaseRelation(parameters)(sqlContext)
  }
}
