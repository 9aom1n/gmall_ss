package com.gm.gmallpublisher.service.impl;

import com.gm.gmallpublisher.mapper.DauMapper;
import com.gm.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @Author: Gm
 * @Date: 2021/7/30 12:37
 */

public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }
}
