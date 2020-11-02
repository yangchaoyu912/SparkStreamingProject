package com.yuwenzhi.project

import java.util.Properties

import com.yuwenzhi.Utils.{MyKafkaUtil, PropertiesUtil}
import com.yuwenzhi.handler.{BlackListHandler, DtAreaCityAdCountHandler, LastHourAdCountHandler}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description:
 * @author: 宇文智
 * @date 2020/11/2 15:25
 * @version 1.0
 */
object RealTimeApp {
  def main(args: Array[String]): Unit = {
    val ssconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
    val ssc = new StreamingContext(ssconf,Seconds(3))

    //读取配置信息
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")

    //连接kafka读取数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc)
    //将kafka数据封装成样例类
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(
      record => {
        val value: String = record.value()
        val arr: Array[String] = value.split(" ")
        Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
      }
    )
    //========需求一: 广告黑名单======
    //1. 根据黑名单，过滤掉黑名单用户数据集
    val filterDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream)
    //2.将数据写入user_ad_count表， 再统计当前用户累计点击广告是否大于30的 写入黑名单
    BlackListHandler.addBlackList(filterDStream)
    //========需求二： 统计每天每地区每城市的广告点击量====
    //对过滤掉黑名单用户的数据集统计
    DtAreaCityAdCountHandler.saveDtAreaCityAdCountToMysql(filterDStream)

    //=======需求三： 最近一小时（测试 2分钟）的广告点击量
    LastHourAdCountHandler.saveAdCountLastHourToMysql(filterDStream).print()


    ssc.start() //任务启动
    ssc.awaitTermination() //将主线程阻塞，不退出
  }
}

//时间戳 地区 城市 用户id 广告id
case class Ads_log(timestamp: Long,area: String,city: String,userId: String,adid:String)
