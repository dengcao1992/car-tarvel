package com.cartravel.spark

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/06 14:30
  * @note 一些值得注意的地方
  */
class HaiKouOrderParser extends OrderParser{
    //todo: 这儿既然会返回null，那用Option明显更好
    def parser(orderInfo: String): Option[TravelOrder] = {
        //文件中包含文件头，所以需要正则判断是否是数据行
        val regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}".r
        val iterator = regex.findFirstIn(orderInfo)
        if (iterator.isEmpty) {
            return None
        }

        //使用\t进行分割
        val orderInfos = orderInfo.split(" ", -1)
        val order = new HaiKouTravelOrder()

        println("orderInfos:"+orderInfos.size)
        if(null==orderInfos||(orderInfos.size!=26)){
            return None
        }

        order.orderId = orderInfos(0)
        order.productId = orderInfos(1)
        order.cityId = orderInfos(2)
        order.district = orderInfos(3)
        order.county = orderInfos(4)
        order.orderTimeType = orderInfos(5)
        order.comboType = orderInfos(6)
        order.trafficType = orderInfos(7)
        order.passengerCount = orderInfos(8)
        order.driverProductId = orderInfos(9)
        order.startDestDistance = orderInfos(10)
        order.arriveDay = orderInfos(11)+""
        order.arriveTime = orderInfos(12)+""
        order.departureDay = orderInfos(13)+""
        order.departureTime = orderInfos(14)+""
        order.preTotalFee = orderInfos(15)
        order.normalTime = orderInfos(16)
        order.bubbleTraceId = orderInfos(17)
        order.productLlevel = orderInfos(18)
        order.destLng = orderInfos(19)
        order.destLat = orderInfos(20)
        order.startingLng = orderInfos(21)
        order.startingLat = orderInfos(22)
        order.year = orderInfos(23)
        order.month = orderInfos(24)
        order.day = orderInfos(25)

        //订单数据中没有实际产生时间,可以按照订单出发时间作为订单的产生时间
        order.createDay=order.departureDay

        Some(order)
    }

}

