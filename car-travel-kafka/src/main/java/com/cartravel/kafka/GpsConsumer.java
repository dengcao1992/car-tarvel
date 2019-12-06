package com.cartravel.kafka;

import com.cartravel.common.Constants;
import com.cartravel.common.Order;
import com.cartravel.common.TopicName;
import com.cartravel.util.HBaseUtil;
import com.cartravel.util.JedisUtil;
import com.cartravel.util.ObjUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 功能描述
 *
 * @author dcs
 * @version 0.0
 * @tparam T 构造泛型参数
 * @note 一些值得注意的地方
 * @since 2019/12/06 15:59
 */
public class GpsConsumer {
}
