package com.cartravel.hbase.com.cartravel.hbase.org.custom.spark.sql.hbase

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.sources.TableScan

import scala.reflect.internal.util.TableDef.Column

/**
  * Created by angel
  */
object Resolver extends Serializable {

  //解析rowkey
  private def resolveRowkey[T<:Result , S<:String](result:Result , resultType:String):Any = {
    val  rowkey = resultType match{
      case "String" => result.getRow.map(_.toChar).mkString.toString
      case "Int" => result.getRow.map(_.toChar).mkString.toInt
      case "Long" => result.getRow.map(_.toChar).mkString.toLong
      case "Double" => result.getRow.map(_.toChar).mkString.toDouble
      case "Float" => result.getRow.map(_.toChar).mkString.toFloat
    }
    rowkey
  }


  //列和列族
  private def resolveColumn[T<:Result , S<:String , A<:String , B<:String](result: Result ,
                                                                           columnFamily:String ,
                                                                           columnName:String ,
                                                                           resultType:String):Any = {
    //确保传入的columnFamily ， 应该在result结果集里面
    val column = result.containsColumn(columnFamily.getBytes , columnName.getBytes) match{
      case true =>
        resultType match {
          case "String" =>Bytes.toString(result.getValue(Bytes.toBytes(columnFamily) , Bytes.toBytes(columnName)))
          case "Int" =>Bytes.toInt(result.getValue(Bytes.toBytes(columnFamily) , Bytes.toBytes(columnName)))
          case "Double" =>Bytes.toDouble(result.getValue(Bytes.toBytes(columnFamily) , Bytes.toBytes(columnName)))
          case "Long" =>Bytes.toLong(result.getValue(Bytes.toBytes(columnFamily) , Bytes.toBytes(columnName)))
          case "Float" =>Bytes.toFloat(result.getValue(Bytes.toBytes(columnFamily) , Bytes.toBytes(columnName)))
        }
      case _ =>
        resultType match{
          case "String" => ""
          case "Int" => 0
          case "Long" => 0l
          case "Double" => 0.0
        }
    }

    column
  }


  //对外提供服务
  def resolve(hbaseSchemaField: HbaseSchemaField , result: Result):Any = {
    //cf:field1 cf:field2
    val split: Array[String] = hbaseSchemaField.fieldName.split(":" , -1)
    val cfName = split(0)
    val colName = split(1)

    //把rowkey或者列 返回
    var resolveResullt:Any = null
    if(cfName == "" && colName=="key"){
      resolveResullt = resolveRowkey(result , hbaseSchemaField.fieldType)
    }else{
      resolveResullt = resolveColumn(result , cfName , colName , hbaseSchemaField.fieldType)
    }
    resolveResullt

  }

}


