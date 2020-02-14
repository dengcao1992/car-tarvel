package com.cartravel.spark.kafka

import java.time.Duration

import com.cartravel.hbase.conn.HbaseConnections
import com.cartravel.hbase.hutils.HbaseTools
import com.cartravel.loggings.Logging
import com.cartravel.utils.GlobalConfigUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import collection.JavaConversions._
import scala.reflect.ClassTag

/** 功能描述
 *
 * @param hbaseHost   hbase地址
 * @param kafkaParams kafka配置参数
 * @author dcs
 * @version 0.0
 * @since 2020/02/10 18:14
 * @note 一些值得注意的地方
 */
class KafkaManagerDependHbase(hbaseHost: String, zkHost: String, kafkaParams: Map[String, Object]) extends IKafkaManager {
    val columnFamily = GlobalConfigUtils.heartColumnFamily
    val tableName = "kafka_offset"
    val conf = Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> kafkaParams(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
    val adminClient = AdminClient.create(conf)
    
    override def createDirectStream[K: ClassTag, V: ClassTag](ssc: StreamingContext, topics: Seq[String]): InputDStream[ConsumerRecord[K, V]] = {
        val topicPartition: Map[TopicPartition, Long] = readOffset(topics, kafkaParams("group.id").toString)
        KafkaUtils.createDirectStream(ssc, PreferConsistent, ConsumerStrategies.Subscribe[K, V](topics, kafkaParams, topicPartition))
    }
    
    override def persistOffset[K, V](rdd: RDD[ConsumerRecord[K, V]], storeOffset: Boolean): Unit = {
        val groupId = kafkaParams("group.id").toString
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val conn = HbaseConnections.getHbaseConn
        try {
            offsetRanges.foreach(offset => {
                val rowkey = s"${offset.topic}#${offset.partition}#$groupId"
                val column = if(storeOffset) offset.untilOffset else offset.fromOffset
                HbaseTools.putMapData(conn, tableName, 8, columnFamily, "", Map("offset" -> column.toString))
            })
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            HbaseConnections.closeConn(conn)
        }
       
    }
    
    def readOffset(topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
        val earliestOffsets = getEarliestOffsets(kafkaParams , topics)
        val latestOffsets = getLatestOffsets(kafkaParams , topics)
        adminClient.describeTopics(topics).values()
                .flatMap { case (topic, describe) =>
                    describe.get().partitions().map(topicPartitionInfo => {
                        val topicPartition = new TopicPartition(topic, topicPartitionInfo.partition())
                        val offset = try {
                            readOffsetFromHbase(topic, topicPartitionInfo.partition(), groupId)
                                    .getOrElse(readOffsetFromKafka(topic, topicPartitionInfo.partition(), groupId))
                        } catch {
                            case e: Exception =>
                                e.printStackTrace()
                                readOffsetFromKafka(topic, topicPartitionInfo.partition(), groupId)
                        }
                        val earliest = earliestOffsets(topicPartition)
                        val latest = latestOffsets(topicPartition)
                        (topicPartition, if(offset < earliest || offset > latest) earliest else offset)
                    })
                }
                .toMap
    }
    
    /** 功能描述
     *
     * @param topic  参数描述
     * @param groupId 参数描述
     * @return Option[Long]
     * @throws
     * @author dengc
     * @version 0.0
     * @since 2020/2/12 15:34
     * @note 一些值得注意的地方
     * @example {{{这是一个例子}}}
     */
    private def readOffsetFromHbase(topic: String, partition: Int, groupId: String): Option[Long] = {
        val conn = HbaseConnections.getHbaseConn
        try {
            val rowkey = s"$topic#$partition#$groupId"
            HbaseTools.getMapData(conn, tableName, rowkey, columnFamily, List("offset")).get("offset") match {
                case Some(offset) => Some(offset.toLong)
                case _ => None
            }
        } catch {
            case e: Exception =>
                e.printStackTrace()
                None
        } finally {
            HbaseConnections.closeConn(conn)
        }
        
    }
    
    /** 功能描述
      *
     
      * @param topic 参数描述
      * @param partition 参数描述
      * @param groupId 参数描述
      * @return Long
      * @throws
      * @author dengc
      * @version 0.0
      * @since 2020/2/12 18:07
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    private def readOffsetFromKafka(topic: String, partition: Int, groupId: String): Long = {
        val consumer = new KafkaConsumer[String , Object](kafkaParams)
        val topicCollection = List(new TopicPartition(topic, partition))
        consumer.assign(topicCollection)
        try {
            consumer.beginningOffsets(topicCollection).values().head
        } catch {
            case e: Exception =>
                e.printStackTrace()
                0L
        } finally {
            consumer.close()
        }
    }
    
    /** 功能描述
     *
     * @param kafkaParams 参数描述
     * @param topics      参数描述
     * @return Unit
     * @throws
     * @author dengc
     * @version 0.0
     * @since 2020/2/12 15:44
     * @note 一些值得注意的地方
     * @example {{{这是一个例子}}}
     */
    private def getEarliestOffsets(kafkaParams: Map[String, Object], topics: Seq[String]): Map[TopicPartition, Long] = {
        val conf = collection.mutable.Map(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest") ++ kafkaParams
        val consumer = new KafkaConsumer[String , Array[Byte]](conf)
        consumer.subscribe(topics)
        try{
            consumer.poll(Duration.ofMillis(0))
        }catch{
            case e: Exception => e.printStackTrace()
        }
        val topicp = consumer.assignment().toSet
        consumer.pause(topicp)
        consumer.seekToBeginning(topicp)
        val earliestOffsetMap = topicp.map(line => line -> consumer.position(line)).toMap
        consumer.unsubscribe()
        consumer.close()
        earliestOffsetMap
    }
    
    /** 功能描述
     *
     * @param kafkaParams 参数描述
     * @param topics      参数描述
     * @return Unit
     * @throws
     * @author dengc
     * @version 0.0
     * @since 2020/2/12 15:44
     * @note 一些值得注意的地方
     * @example {{{这是一个例子}}}
     */
    private def getLatestOffsets(kafkaParams: Map[String, Object], topics: Seq[String]): Map[TopicPartition, Long] = {
        val conf = collection.mutable.Map(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest") ++ kafkaParams
        val consumer = new KafkaConsumer[String , Array[Byte]](conf)
        consumer.subscribe(topics)
        try{
            consumer.poll(Duration.ofMillis(0))
        }catch{
            case e: Exception => e.printStackTrace()
        }
        val topicp = consumer.assignment().toSet
        consumer.pause(topicp)
        consumer.seekToEnd(topicp)
        val toMap = topicp.map(line => line -> consumer.position(line)).toMap
        val earliestOffsetMap = toMap
        consumer.unsubscribe()
        consumer.close()
        earliestOffsetMap
    }
}

