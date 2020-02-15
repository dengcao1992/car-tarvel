package com.cartravel.hbase.com.cartravel.hbase.org.custom.spark.sql.hbaseV2

import com.cartravel.hbase.com.cartravel.hbase.org.custom.spark.sql.hbase.{HbaseSchemaField, RegisterSchemaField}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.hbase.types.SchemaConverters.SchemaType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.mutable

/** 功能描述
 *
 * @param hbaseConfig 构造参数
 * @param sqlContext  构造参数
 * @author dcs
 * @version 0.0
 * @since 2020/02/14 15:38
 * @note 一些值得注意的地方
 */
case class HbaseRelation(@transient val hbaseConfig: Map[String, String])
                        (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {
    
    override def schema: StructType = {
        val hbaseTableSchemaFromConfig = hbaseConfig.getOrElse("hbase_table_schema", sys.error("need habase schema"))
                .trim.drop(1).dropRight(1).split(",").
                map(field => {
                    HbaseSchemaField(field.trim, "")
                })
        val dfSchema = hbaseConfig.getOrElse("sparksql_table_schema", sys.error("need sparl schema"))
                .trim.drop(1).dropRight(1).split(",").
                map(field => {
                    val nameAndType = field.trim.split("\\s+", -1)
                    RegisterSchemaField(nameAndType.head, nameAndType(1))
                })
        val hbaseTableSchema = addHbaseTypeWithRegisterSchema(hbaseTableSchemaFromConfig, dfSchema)
        val fields: Array[StructField] = hbaseTableSchema.map { case(field, registerField) =>
            val relationType = field.fieldType match {
                case "String" => SchemaType(StringType, nullable = false)
                case "Int" => SchemaType(IntegerType, nullable = false)
                case "Long" => SchemaType(LongType, nullable = false)
                case "Double" => SchemaType(DoubleType, nullable = false)
            }
        
            StructField(registerField.fieldName, relationType.dataType, relationType.nullable)
        }
        StructType(fields)
    }
    
    override def buildScan(): RDD[Row] = ???
    
    private def addHbaseTypeWithRegisterSchema[T<: HbaseSchemaField, S<: RegisterSchemaField](tmpHbaseSchemaField: Array[T], registerTableFields: Array[S]): Array[(T, S)] = {
        if(tmpHbaseSchemaField.length != registerTableFields.length){
            sys.error("两个scchema不一致")
        }
        tmpHbaseSchemaField.zip(registerTableFields).map{
            case (sourceField, registerFiled) => {
                (sourceField.copy(fieldType = registerFiled.fieldType).asInstanceOf[T], registerFiled)
            }
        }
    }
}
