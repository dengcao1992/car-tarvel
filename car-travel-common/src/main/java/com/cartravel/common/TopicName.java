package com.cartravel.common;

import java.io.Serializable;

/**
 * 功能描述
 *
 * @author dcs
 * @version 0.0
 * @tparam T 构造泛型参数
 * @note 一些值得注意的地方
 * @since 2019/12/06 11:02
 */
public enum TopicName implements Serializable {
    //海口市订单主题
    HAI_KOU_ORDER_TOPIC("hai_kou_order_topic","海口市订单主题"),

    //成都市订单主题
    CHENG_DU_ORDER_TOPIC("cheng_du_order_topic","成都市订单主题"),

    //西安市订单主题
    XI_AN_ORDER_TOPIC("xi_an_order_topic","西安市订单主题"),

    //西安市轨迹主题
    XI_AN_GPS_TOPIC("xi_an_gps_topic","西安市轨迹主题"),

    //海口市轨迹主题
    CHENG_DU_GPS_TOPIC("cheng_du_gps_topic","海口市轨迹主题");

    /**
     * 主题名
     */
    private String name;

    /**
     * 主题描述
     */
    private String desc;

    TopicName(String name,String desc){
        this.name=name;
        this.desc = desc;
    }

    public String getDesc(){
        return desc;
    }

    /**
     * 返回主题名称
     * @return string
     */
    public String getTopicName(){
        return name;
    }
}
