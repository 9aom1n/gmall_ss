package com.gm.bean

/**
 * @Author: Gm
 * @Date: 2021/8/3 9:05
 */

case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)