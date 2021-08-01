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
}
