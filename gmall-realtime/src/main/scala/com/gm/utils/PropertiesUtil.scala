package com.gm.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Author: Gm
 * @Date: 2021/7/29 16:47
 */

object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}

