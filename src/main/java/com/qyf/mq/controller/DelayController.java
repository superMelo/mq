package com.qyf.mq.controller;


import com.alibaba.fastjson.JSON;
import com.qyf.mq.delay.RedisClient;
import com.qyf.mq.entity.Content;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@Slf4j
public class DelayController {

    @Autowired
    private RedisClient redisClient;
    private static final String key = "delay";

    /**
     * 往zset里按时间添加消息
     * @return
     */
    @RequestMapping("product")
    public Set product(){
        Date date = new Date();
        Content content = new Content();
        content.setId(UUID.randomUUID().toString());
        content.setState("1");
        content.setContent("delay message");
        content.setTime(date);
        redisClient.add(key, JSON.toJSONString(content), date.getTime());
        Set range = redisClient.range(key, 0, 1000);
        return range;
    }

    //开启线程池，不断消费zset里面最新的消息
    @RequestMapping("consumer")
    public void consumer(){
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 60L,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(4000), new ThreadFactory() {
            AtomicInteger  poolNumber = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "delay" + poolNumber.getAndIncrement());
            }
        } , new ThreadPoolExecutor.AbortPolicy());
        executor.execute(()->{
            while (true){
                Set delay = redisClient.range(key, 0, 1);
                if (delay != null && delay.size() > 0){
                    Object[] objects = delay.toArray();
                    Date date = new Date();
                    Content content = JSON.parseObject(objects[0].toString(), Content.class);
                    //根据时间判断是否执行
                    if (date.after(content.getTime())){
                        log.info("obj:{}", content);
                        redisClient.del(key, objects[0]);
                    }
                }
            }
        });
    }
}
