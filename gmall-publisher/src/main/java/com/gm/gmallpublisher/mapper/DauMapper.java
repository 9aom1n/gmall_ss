package com.gm.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author: Gm
 * @Date: 2021/7/30 12:38
 */
public interface DauMapper {
    public Integer selectDauTotal(String date);
    public List<Map> selectDauTotalHourMap(String date);
}
