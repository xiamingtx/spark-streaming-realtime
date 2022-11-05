package com.xm.springboot.springbootdemo.mapper;

import com.xm.springboot.springbootdemo.bean.Customer;

/**
 * 数据层接口
 *
 * 目前 数据层一般都是基于MyBatis实现的 MyBatis的玩法是只写接口+SQL即可 不需要实现类 这里用ES 所以需要实现类
 * @author 夏明
 * @version 1.0
 */
public interface CustomerMapper {
    Customer searchByUsernameAndPassword(String username, String password);
}
