package com.gm.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.gm.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Gm
 * @Date: 2021/7/30 12:34
 */

public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realTimeTotal(@RequestParam("date") String date) {
        //从service层获取日活总数数据
        int dauTotal = publisherService.getDauTotal(date);

        //创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //创建map几个用来存放具体数据
        //存放新增日活的map合集
        HashMap<String, Object> dauMap = new HashMap<>();

        //存放新增设备的马集合
        HashMap<String, Object> devMap = new HashMap<>();

        //将数据封装到Map合集中
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_id");
        devMap.put("name","新增设备");
        devMap.put("value", 233);

        //将map集合放入List集合
        result.add(dauMap);
        result.add(devMap);

        return JSONObject.toJSONString(result);

    }
}
