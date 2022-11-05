package com.xm.springboot.springbootdemo.service;

import org.springframework.stereotype.Service;

/**
 * 业务层接口
 * @author 夏明
 * @version 1.0
 */
public interface CustomerService {
    public String doLogin(String username, String password);
}
