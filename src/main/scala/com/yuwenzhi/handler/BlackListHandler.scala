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
 * @date 2020/11/2 15:38
 * @version 1.0
 */
object BlackListHandler {

  //时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")


  def addBlackList(filterDStream: DStream[Ads_log]) = {


      //0. 时间格式化和结构转化
      //统计当前批次中单日每个用户点击每个广告的总次数
    val oneClickDStream: DStream[((String,String, String), Int)] = filterDStream.map(log => {
      val dt: String = sdf.format(new Date(log.timestamp))
      ((dt, log.userId, log.adid), 1) //一条记录一个点击数据
    }).reduceByKey(_+_)
      //1 将数据更新写入user_ad_count表
    oneClickDStream.foreachRDD(
        rdd => {

          rdd.foreachPartition(
            //将集合数据写入表中
            iter => {
              //创建connection
              val connection: Connection = JDBCUtil.getConnection
              iter.foreach{
                case (((dt,user,adid),sum))=>{
                  //user_ad_count表中更新累加点击次数
                  JDBCUtil.executeUpdate(
                    connection,
                    """
                      |insert into user_ad_count(dt,userid,adid,count)
                      |values(?,?,?,?)
                      |on duplicate key
                      |update count = count + ?
                      |""".stripMargin,
                    Array(dt,user,adid,sum,sum)
                  )
                  //每批次查询一次user_ad_count表
                  //2. 统计当前用户的累计点击广告次数是否大于30的 ，大于就写入黑名单
                  val count: Long = JDBCUtil.getDataFromMysql(
                    connection,
                    """
                      |select count from user_ad_count where userid = ? and adid = ?
                      |""".stripMargin,
                    Array(user, adid)
                  )
                  if (count>30) {
                    JDBCUtil.executeUpdate(
                      connection,
                      """
                        |insert into black_list (userid)
                        |values(?)
                        |on duplicate key update userid = ?
                        |""".stripMargin,
                      Array(user,user)
                    )
                  }
                }
              }
              connection.close()
            }
          )
        }
      )

  }

  def filterByBlackList(adsLogDStream: DStream[Ads_log]) = {
    adsLogDStream.transform(
      rdd => {
        rdd.filter(
          ads_log => {
            val connection: Connection = JDBCUtil.getConnection
            val isExist: Boolean = JDBCUtil.isExist(
              connection,
              "select * from black_list where userid = ?",
              Array(ads_log.userId)
            )
            connection.close()
            //存在的不要
            !isExist
          }
        )
      }
    )
  }

}
