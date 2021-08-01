package com.gm.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.gm.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Gm
 * @Date: 2021/7/30 12:34
 */
@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realTimeTotal(@RequestParam("date") String date) {
        //从service层获取日活总数数据
        int dauTotal = publisherService.getDauTotal(date);

        Double amountTotal = publisherService.getOrderAmountTotal(date);

        //创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //创建map几个用来存放具体数据
        //存放新增日活的map合集
        HashMap<String, Object> dauMap = new HashMap<>();

        //存放新增设备的马集合
        HashMap<String, Object> devMap = new HashMap<>();

        //存放交易额总数的map合集
        HashMap<String, Object> gmvMap = new HashMap<>();

        //将数据封装到Map合集中
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_id");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", amountTotal);

        //将map集合放入List集合
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    //获取分时数据方法
    @RequestMapping("realtime-hours")
    public String getDauHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        //获取service 返回的数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map todayHourMap = null;
        Map yesterdayHourMap = null;
        if ("dau".equals(id)) {
            //获取今天的数据
            todayHourMap = publisherService.getDauHour(date);
            //获取昨天的数据
            yesterdayHourMap = publisherService.getDauHour(yesterday);
        } else if ("order_amount".equals(id)) {
            //获取今天交易额数据
            todayHourMap = publisherService.getOrderAmountHourMap(date);
            //获取昨天的交易额数据
            yesterdayHourMap = publisherService.getOrderAmountHourMap(yesterday);
        }
        //创建存放最终结果的map集合
        HashMap<String, Map> result = new HashMap<>();

        result.put("yesterday", yesterdayHourMap);
        result.put("today", todayHourMap);
        return JSONObject.toJSONString(result);
    }
}
