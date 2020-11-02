package com.yuwenzhi.Utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @description:
 * @author: 宇文智
 * @date 2020/11/2 15:11
 * @version 1.0
 */
object PropertiesUtil {
  def load(propertiesName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().
      getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    prop
  }
}
