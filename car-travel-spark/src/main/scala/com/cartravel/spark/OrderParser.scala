package com.cartravel.spark

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/06 14:32
  * @note 一些值得注意的地方
  */
trait OrderParser {
    def parser(orderInfo: String): Option[TravelOrder]
}
