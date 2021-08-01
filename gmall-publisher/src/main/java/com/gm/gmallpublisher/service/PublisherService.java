package com.gm.gmallpublisher.service;

import java.util.Map;

/**
 * @Author: Gm
 * @Date: 2021/7/30 12:36
 */

public interface PublisherService {
    //获取日活总数数据
     public int getDauTotal(String date);
    // 获取分时数据抽象方法
    public Map getDauHour(String date);

    //获取Gmv每日总数的抽象方法
    public Double getOrderAmountTotal(String date);

    //获取GMV 每日交易额分时数据
    public Map<String , Double> getOrderAmountHourMap(String date);
}
