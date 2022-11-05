package com.xm.springboot.springbootdemo.bean;

import lombok.*;

/**
 * @author 夏明
 * @version 1.0
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class Customer {
    private String username;
    private String password;
    private String address;
    private Integer age;

    // 构造器(无参+有参)
    // getter/setter
    // toString
    // ......
}
