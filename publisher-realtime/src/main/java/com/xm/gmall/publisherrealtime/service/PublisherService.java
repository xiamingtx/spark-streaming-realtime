package com.xm.gmall.publisherrealtime.service;

import com.xm.gmall.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

/**
 * @author 夏明
 * @version 1.0
 */
public interface PublisherService {
    Map<String, Object> doDauRealtime(String td);

    List<NameValue> doStatsByItem(String itemName, String date, String t);

    Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize);
}
