package com.cartravel.hbase.hutils

import com.cartravel.loggings.Logging
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, Get, Put, Table}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by angel
 */
object HbaseTools extends Logging {
    //建表
    private def createTableBySelfBuilt(conn: Connection, tableName: TableName, retionNum: Int, column: Array[String]) = {
        this.synchronized {
            val admin = conn.getAdmin
            try {
                if (!admin.tableExists(tableName)) {
                    //表描述器
                    val tbDesc = new HTableDescriptor(tableName)
                    if (column != null) { //如果列族不为空，则迭代
                        column.foreach(c => {
                            val hcd = new HColumnDescriptor(c.getBytes())
                            hcd.setBlockCacheEnabled(false)
                            hcd.setMaxVersions(1)
                            hcd.setBloomFilterType(BloomType.ROW)
                            hcd.setCompressionType(Algorithm.SNAPPY)
                            tbDesc.addFamily(hcd)
                        })
                    }
                    
                    val splitKeysBuilder = new SplitKeysBuilder()
                    val splitKeys = splitKeysBuilder.getSplitKeys(retionNum)
                    admin.createTable(tbDesc, splitKeys)
                }
            } catch {
                case e: Exception => // 邮件报警
            }
        }
    }
    
    //插入数据
    def putMapData(
                          connection: Connection,
                          tableName: String,
                          regionNum: Int,
                          columnFamily: String,
                          key: String,
                          mapData: Map[String, String]
                  ) = {
        
        val admin = connection.getAdmin
        val tb = TableName.valueOf(tableName)
        
        val table: Table = connection.getTable(tb)
        try {
            val rk = RowkeyUtils.getRowkey(key)
            val rowkey = Bytes.toBytes(rk)
            val put = new Put(rowkey)
            if (mapData.size > 0) {
                for ((k, v) <- mapData) {
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k), Bytes.toBytes(v.toString))
                }
            }
            table.put(put)
        } catch {
            case e: Exception =>
            //邮件
        } finally {
            table.close()
        }
    }
    
    /** 功能描述
     *
     * @param connection 参数描述
     * @param tableName  参数描述
     * @param rowkey     参数描述
     * @param family     参数描述
     * @param columns    参数描述
     * @return _root_.scala.Predef.Map[_root_.scala.Predef.String, _root_.scala.Predef.String]
     * @throws
     * @author dengc
     * @version 0.0
     * @since 2020/2/12 16:53
     * @note 一些值得注意的地方
     * @example {{{这是一个例子}}}
     */
    def getMapData(
                          connection: Connection,
                          tableName: String,
                          rowkey: String,
                          family: String,
                          columns: Seq[String]
                  ): Map[String, String] = {
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        val get = new Get(Bytes.toBytes(rowkey))
        get.addFamily(family.getBytes())
        try {
            val result = table.get(get)
            columns.map(col => {
                (col, Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(col))))
            }).toMap
        } catch {
            case e: Exception =>
                e.printStackTrace()
                Map()
        } finally {
            table.close()
        }
        
    }
}
