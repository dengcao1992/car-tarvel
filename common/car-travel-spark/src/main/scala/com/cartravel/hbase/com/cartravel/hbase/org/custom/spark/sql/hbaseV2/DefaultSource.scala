package com.cartravel.hbase.com.cartravel.hbase.org.custom.spark.sql.hbaseV2

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

/** 功能描述
 *
 * @author dcs
 * @version 0.0
 * @since 2020/02/12 15:35
 * @note 一些值得注意的地方
 */
class DefaultSource extends RelationProvider{
    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        HbaseRelation(parameters)(sqlContext)
    }
}