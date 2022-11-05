package com.xm.springboot.springbootdemo.controller;

import com.xm.springboot.springbootdemo.bean.Customer;
import com.xm.springboot.springbootdemo.service.CustomerService;
import com.xm.springboot.springbootdemo.service.impl.CustomerServiceImplNew;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

/**
 * 控制层
 *
 * @author 夏明
 * @version 1.0
 */
class MyFactoryUtils{
    public static CustomerService newInstance() {
        // return new CustomerServiceImpl();
        return new CustomerServiceImplNew();
    }
}

//@Controller // 标识为控制层(Spring)
@RestController // @ResponseBody + @Controller
public class CustomerController {

    // 直接在类中创建对象 也不好 写死了
    //  CustomerServiceImpl customerService = new CustomerServiceImpl();
    // CustomerService customerService = new CustomerServiceImpl();

    // 需求: 更换业务层实现: CustomerServiceNew
    // CustomerServiceNew customerService = new CustomerServiceNew();

    // 接口:
    // CustomerService customerService = MyFactoryUtils.newInstance();
    // SpringBoot怎么做?
    @Autowired // 从Spring容器中找到对应类型的对象 注入过来
    @Qualifier("customerServiceImplNew") // 明确指定从哪个对象注入过来
    CustomerService customerService;

    // http://localhost:8080/login?username=zhanqsan&password=123456
    @GetMapping("login")
    public String login(@RequestParam("username") String username, @RequestParam("password") String password) {
        // 业务处理
        // 在每个方法中创建业务层对象 不好
        String result = customerService.doLogin(username, password);
        return result;
    }

    /**
     * 常见的状态码:
     *
     * 200: 表示请求处理成功且响应成功
     * 302: 表示进行重定向
     * 400: 表示参数有误
     * 404: 表示请求地址或资源不存在
     * 405: 表示请求方式不支持
     * 500: 表示服务端处理异常
     *
     * @author 夏明
     * @date 2022/11/5 23:19
     * @param null
     */

    /**
     * 请求方式: GET POST PUT DELETE ...
     *
     * 现在主流的还是两种方式: GET & POST
     *
     * GET: 读
     *
     * POST: 写
     * @author 夏明
     * @date 2022/11/5 22:31
     */
//    @RequestMapping(value = "requestmethod", method=RequestMethod.GET)
    @GetMapping("requestmethod")
    public String requestMethod() {
        return "success";
    }

    /**
     * 请求参数:
     * 1. 地址栏中的kv格式的参数
     * 2. 嵌入到地址栏中的参数
     * 3. 封装到请求体中的参数
     * @author 夏明
     * @date 2022/11/5 19:33
     */

    /**
     * 1．地址栏中的kv格式的参数
     * http://Localhost:8080/paramkv?username=zhangsan&age=22
     * @RequestParam: 将请求参数映射到方法对应的形参上
     * 如果请求参数名和形参名一致 可以直接进行形参映射 省略@RequestParam
     *
     */
    @RequestMapping("paramkv")
    public String paramkv(@RequestParam("username") String name, @RequestParam("age") Integer age) {
        return "name = " + name + " , age = " + age;
    }

    /**
     * 2.族入到地址栏中的参数
     * http://localhost:8080/parampath/lisi/22?address=beijing
     *
     * @PathVariable 将请求路径中的参数映射到方法中的形参上
     */
    @RequestMapping("/parampath/{username}/{age}")
    public String parampath(
            @PathVariable("username") String username,
            @PathVariable("age") Integer age,
            @RequestParam("address") String address) {
        return "username= " + username + ", age= " + age + ", address= " + address;
    }

    /**
     * 3. 封装到请求体中的参数
     *
     * http://localhost:8080/parambody
     *
     * 请求体中的参数:
     * username=xm
     * password=123123
     *
     * 如果请求参数名与请求形参名不一致 需要通过@RequestParam来标识获取
     * 如果一致的话可以直接映射
     *
     * @RequestBody 将请求体中的json格式的参数映射到对象对应的属性上
     * @author 夏明
     * @date 2022/11/5 20:05
     */
    @RequestMapping("parambody")
    public Customer parambody(@RequestBody Customer customer) {
        return customer; // 转换成json字符串返回
    }

//    @RequestMapping("parambody")
//    public String parambody(String username, String password) {
//        return "username= " + username + ", password= " + password;
//    }

    /**
     * 客户端请求: http://localhost:8080/helloworld
     * <p>
     * 请求处理方法
     *
     * @RequestMapping: 将客户端的请求与方法进行映射
     * @ResponseBody: 将方法的返回值处理成字符串(json)返回给客户端
     * @author 夏明
     * @date 2022/11/5 19:28
     */
    @RequestMapping("/helloworld")
    // @ResponseBody
    public String helloWorld() {
        return "success";
    }

    @RequestMapping("/hello")
    public String hello() {
        return "hello Java";
    }
}
