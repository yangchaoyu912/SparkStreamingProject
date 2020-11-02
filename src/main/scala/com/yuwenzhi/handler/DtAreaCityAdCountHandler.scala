package com.yuwenzhi.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.yuwenzhi.Utils.JDBCUtil
import com.yuwenzhi.project.Ads_log
import org.apache.spark.streaming.dstream.DStream

/**
 * @description:
 * @author: 宇文智
 * @date 2020/11/2 18:27
 * @version 1.0
 */
//统计每天每地区每城市的广告点击量，并写入数据库
object DtAreaCityAdCountHandler {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")
  def saveDtAreaCityAdCountToMysql(filterDStream: DStream[Ads_log]) = {
    //思路： 将dt,area,city 转化为key，1 做 value 然后在reduceByKey
    val dataAfterCount: DStream[((String, String, String,String), Int)] = filterDStream.map(
      log => {
        val dt: String = sdf.format(new Date(log.timestamp))
        ((dt, log.area, log.city,log.adid), 1)
      }
    ).reduceByKey(_ + _)
    // 对数据表进行更新
    dataAfterCount.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            //连接数据库
            val connection: Connection = JDBCUtil.getConnection
            iter.foreach{
              case ((dt,area,city,adid),sum) =>{
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    |insert into area_city_ad_count (dt,area,city,adid,count)
                    |values(?,?,?,?,?)
                    |on duplicate key
                    |update count=count + ?
                    |""".stripMargin,
                  Array(dt,area,city,adid,sum,sum)
                )
              }

            }
            connection.close()
          }
        )
      }
    )
  }

}
