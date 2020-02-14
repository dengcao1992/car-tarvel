package com.cartravel.spark.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.reflect.ClassTag

/** 功能描述
 *
 * @param args 构造参数
 * @tparam T 构造泛型参数
 * @author dcs
 * @version 0.0
 * @since 2020/02/10 18:16
 * @note 一些值得注意的地方
 */
trait IKafkaManager extends Serializable {
    def createDirectStream[K:ClassTag , V:ClassTag](ssc:StreamingContext , topics:Seq[String]):InputDStream[ConsumerRecord[K, V]]
    def persistOffset[K,V](rdd:RDD[ConsumerRecord[K,V]] ,  storeOffset:Boolean = true)
}
