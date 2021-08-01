package com.gm.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author: Gm
 * @Date: 2021/8/1 9:10
 */

public interface OrderMapper {
    //查询当日交易总额
    public Double selectOrderAmountTotal(String date);
    //查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);
}
