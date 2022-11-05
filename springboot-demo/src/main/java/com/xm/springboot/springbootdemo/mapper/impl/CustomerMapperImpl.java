package com.xm.springboot.springbootdemo.mapper.impl;

import com.xm.springboot.springbootdemo.bean.Customer;
import com.xm.springboot.springbootdemo.mapper.CustomerMapper;
import org.springframework.stereotype.Repository;

/**
 * 数据层组件
 * @author 夏明
 * @version 1.0
 */
@Repository
public class CustomerMapperImpl implements CustomerMapper {
    @Override
    public Customer searchByUsernameAndPassword(String username, String password) {
        System.out.println("CustomerMapperImpl: 数据库的查询操作");
        // JDBC
        // Mybatis
        // Hibernate
        // ...
        return new Customer(username, password, null, null);
    }
}
