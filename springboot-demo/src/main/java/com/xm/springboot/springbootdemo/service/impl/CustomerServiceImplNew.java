package com.xm.springboot.springbootdemo.service.impl;

import com.xm.springboot.springbootdemo.bean.Customer;
import com.xm.springboot.springbootdemo.mapper.CustomerMapper;
import com.xm.springboot.springbootdemo.service.CustomerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author 夏明
 * @version 1.0
 */
@Service
// 默认名字是类名首字母小写形式 -> customerServiceImplNew
public class CustomerServiceImplNew implements CustomerService {
    @Autowired
    public CustomerMapper customerMapper;

    @Override
    public String doLogin(String username, String password) {
        System.out.println("CustomerServiceImplNew : 复杂的业务处理");
        // 数据非空校验
        // 数据格式校验
        // ...

        // 调用数据层 对比数据库中的数据是否一致
        Customer customer = customerMapper.searchByUsernameAndPassword(username, password);
        if (customer != null) {
            return "ok";
        } else {
            return "error";
        }
    }
}
