package com.gm.gmallpublisher.service.impl;

import com.gm.gmallpublisher.mapper.DauMapper;
import com.gm.gmallpublisher.mapper.OrderMapper;
import com.gm.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Gm
 * @Date: 2021/7/30 12:37
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;

    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHour(String date) {
        //1. 获取Mapper查出来的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        //2. 遍历list集合拿出每一个map将其重组成新的map
        HashMap<String, Long> resultMap = new HashMap<>();
        for (Map map : list) {
            resultMap.put((String)map.get("LH"), (Long)map.get("CT"));
        }
        return resultMap;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
        //获取数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        //创建新的map用于结果数据
        HashMap<String, Double> result = new HashMap<>();

        //遍历集合将老Map的数据转换结构存入新的map
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

}
