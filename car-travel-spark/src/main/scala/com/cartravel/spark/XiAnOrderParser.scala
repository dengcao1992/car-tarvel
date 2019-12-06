package com.cartravel.spark

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/06 14:39
  * @note 一些值得注意的地方
  */
class XiAnOrderParser extends OrderParser {
    override def parser(orderInfo: String): Option[TravelOrder] = {
        val orderInfos = orderInfo.split(",", -1)
        val order = new XiAnTravelOrder()
        order.orderId= orderInfos.head
        Some(order)
    }
}
