package com.yuwenzhi.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.yuwenzhi.project.Ads_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

/**
 * @description:
 * @author: 宇文智
 * @date 2020/11/2 20:03
 * @version 1.0
 */
object LastHourAdCountHandler {
  /**
   * 广告1：  1：List [15:50->10,15:51->25,15:52->30]
   * 广告2：  2：List [15:50->10,15:51->25,15:52->30]
   * 广告3：  3：List [15:50->10,15:51->25,15:52->30]
   */
    private val sdf = new SimpleDateFormat("HH:mm")
  def saveAdCountLastHourToMysql(filterDStream: DStream[Ads_log]) = {
    //日志信息： 某时刻 某地区 某城市 某人 点击广告id
    val hourAdCountDStream: DStream[((String, String), Int)] = filterDStream.map(
      log => {
        val hm: String = sdf.format(new Date(log.timestamp))
        ((hm, log.adid), 1)
      }
    )
    //使用窗口 统计2分钟的数据，滑步也为2分钟
    val resultCountAfterGroupDStream: DStream[(String, Iterable[(String, Int)])] =
        hourAdCountDStream.window(Minutes(2)).reduceByKey(_ + _).map {
          //聚合完后结构转化
          case ((hm, ad), sum) => {
            (ad, (hm, sum))
          }
        }.groupByKey()
    //对组内元素排序
    resultCountAfterGroupDStream.mapValues(iter=>{iter.toList.sortWith(_._2<_._2)})
  }

}
