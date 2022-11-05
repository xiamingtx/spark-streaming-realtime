package com.xm.springboot.springbootdemo.service.impl;

import com.xm.springboot.springbootdemo.service.CustomerService;
import org.springframework.stereotype.Service;

/**
 * @author 夏明
 * @version 1.0
 */
@Service // 标识成业务层组件(Spring) -> Spring会自动创建该类的对象(单例) 并管理到Spring容器中
// 默认名字是类名首字母小写形式 -> customerServiceImpl
// @Service(value = "csi") // 可以这样设置名字
public class CustomerServiceImpl implements CustomerService {


    public String doLogin(String username, String password) {
        System.out.println("CustomerServiceImpl : 复杂的业务处理");
        // 数据非空校验
        // 数据格式校验
        // ...

        // 调用数据层 对比数据库中的数据是否一致
        return "ok"; // error
    }
}
