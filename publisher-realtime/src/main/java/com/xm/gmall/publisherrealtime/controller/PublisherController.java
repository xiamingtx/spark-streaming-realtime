package com.xm.gmall.publisherrealtime.controller;

import com.xm.gmall.publisherrealtime.bean.NameValue;
import com.xm.gmall.publisherrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author 夏明
 * @version 1.0
 */
@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    /**
     * 交易分析明细查询
     * @author 夏明
     * @date 2022/11/6 20:17
     * @param date
     * @param itemName
     * @param pageNo
     * @param pageSize
     * @return Map<Object>
     */
    @GetMapping("detailByItem")
    public Map<String, Object> detailByItem(
            @RequestParam("date") String date,
            @RequestParam("itemName") String itemName,
            @RequestParam(value = "pageNo", required = false, defaultValue = "1") Integer pageNo,
            @RequestParam(value = "pageSize", required = false, defaultValue = "20") Integer pageSize) {
        return publisherService.doDetailByItem(date, itemName, pageNo, pageSize);
    }

    /**
     * 交易分析 - 按照类别(年龄、性别)统计
     * @author 夏明
     * @date 2022/11/6 19:17
     * @param itemName
     * @param date
     * @param t
     * @return List<NameValue>
     */
    @GetMapping("statsByItem")
    public List<NameValue> statsByItem(
            @RequestParam("itemName") String itemName,
            @RequestParam("date") String date,
            @RequestParam("t") String t) {
        return publisherService.doStatsByItem(itemName, date, t)
    }

    /**
     * 日活分析
     * @author 夏明
     * @date 2022/11/6 19:15
     * @param td
     * @return Map<Object>
     */
    @GetMapping("dauRealtime")
    public Map<String, Object> dauRealtime(@RequestParam("td") String td) {
        return publisherService.doDauRealtime(td);
    }
}
